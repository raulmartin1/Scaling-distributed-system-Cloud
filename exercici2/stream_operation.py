from threading import Lock
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import time
import pika
import boto3

# Configurar log per veure resultats
logging.basicConfig(filename='stream_results.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Funcio que retorna el nombre de missatges pendents a la cua de Rabbitmq
def get_queue_length(channel, queue_name: str) -> int:
    try:
        queue_info = channel.queue_declare(queue=queue_name, durable=True, passive=True) # Nomes crearla si no existeix
        return queue_info.method.message_count
    except Exception as e:
        logging.error(f"Error al obtenir llargada de la cua: {str(e)}")
        return 0

# Funcio per invocar una funcio Lambda de AWS
def invoke_lambda(client, function_name: str, payload: dict) -> None:
    try:
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',      # Execucio asincrona
            Payload=json.dumps(payload)  # Dades a enviar en format json
        )
        logging.info(f"Lambda '{function_name}' invocada amb payload: {payload}")
    except Exception as e:
        logging.error(f"Error al invocar Lambda: {str(e)}")

# Funcio principal que gestiona el proces de stream
def stream(function_name: str, maxfunc: int, rabbitmq_host: str, queue_name: str, 
           user: str, password: str, check_interval: float = 1.0) -> None:
    
    # Crea un canal i declara la cua (durable=True -> per persistencia)
    try:
        credentials = pika.PlainCredentials(user, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=rabbitmq_host, port=5672, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        logging.info("Connexio a RabbitMQ establerta")
    except Exception as e:
        logging.error(f"Error al connectar amb RabbitMQ: {str(e)}")
        return

    # Configuracio del client Lambda
    try:
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        logging.info("Client Lambda configurat")
    except Exception as e:
        logging.error(f"Error al configurar client Lambda: {str(e)}")
        connection.close()  # Tanca connexio RabbitMQ si hi ha error
        return

    # Per controlar la execucio paralela
    active_instances = 0  # Numero d'execucions actives
    lock = Lock()  # Mutex per sincronitzar el acces a active_instances

    # Funcio callback que es crida quan una execucio Lambda acaba
    def decrement_active_instances():
        nonlocal active_instances
        with lock:
            active_instances -= 1     # Decrementa el comptador d'instancies actives cada vegada que es crida
            logging.info(f"Worker finalitzat. Instancies actives: {active_instances}")

    # Permet executar fins a 'maxfunc' ordres en paralel
    with ThreadPoolExecutor(max_workers=maxfunc) as executor:
        logging.info(f"Stream iniciat. Funcio: {function_name}, Maxfunc: {maxfunc}, Cua: {queue_name}")

        while True:
            try:
                # Obtenim nombre de missatges pendents a la cua
                message_count = get_queue_length(channel, queue_name)
                logging.info(f"Missatges a la cua: {message_count}, Instancies actives: {active_instances}")

                # Si hi ha missatges i capacitat per processar-ne
                if message_count > 0 and active_instances < maxfunc:
                    # Obte un missatge de la cua
                    method, properties, body = channel.basic_get(queue=queue_name, auto_ack=False)
                    
                    if body:  # Si s'ha rebut un missatge valid
                        mensaje = body.decode()
                        logging.info(f"Missatge rebut: {mensaje}")
                        
                        # Incrementa comptador d'instancies (amb lock -> sincronitzacio)
                        with lock:
                            active_instances += 1
                        
                        payload = {"mensaje_original": mensaje}   # Payload per la Lambda
                        
                        # Executa la Lambda en un thread
                        future = executor.submit(invoke_lambda, lambda_client, function_name, payload)
                        
                        # Afegeix callback per quan acabi de procesarse
                        future.add_done_callback(lambda f: decrement_active_instances())
                        
                        # Confirmacio de rebre del missatge a rabbitmq
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        logging.info("Missatge procesat i confirmat")
                    else:
                        # Si no hi ha missatges se espera
                        time.sleep(check_interval)
                else:
                    # Si no hi ha missatges o capacitat se espera
                    time.sleep(check_interval)

            # Ctrl + c per aturar
            except KeyboardInterrupt:
                logging.info("Interromput. Tancant stream...")
                break
                
            except Exception as e:
                logging.error(f"Error en stream: {str(e)}")
                time.sleep(check_interval)

    connection.close()
    logging.info("Connexio a RabbitMQ tancada")

if __name__ == '__main__':
    stream(
        function_name='lambda_insult_filter',
        maxfunc=5,  # Max 5 execucions a la vegada
        rabbitmq_host='44.201.248.130',
        queue_name='InsultQueue',
        user='user',
        password='password123'
    )
import logging
import pika
import boto3
import json

# Configurar log per veure resultats
logging.basicConfig(filename='results_trigger.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Conexio a RabbitMQ i la Lambda
conexion = pika.BlockingConnection(pika.ConnectionParameters(
    host='44.201.248.130', port=5672, credentials=pika.PlainCredentials('user', 'password123')))
canal = conexion.channel()
canal.queue_declare(queue='InsultQueue', durable=True)
lambda_client = boto3.client('lambda', region_name='us-east-1')

def callback(ch, method, properties, body):
    try:
        mensaje = body.decode()
        logging.info(f"Mensaje recibido: {mensaje}")

        # Invoquem la lambda del insult filter
        payload = {"mensaje_original": mensaje}
        lambda_client.invoke(FunctionName='lambda_insult_filter', InvocationType='Event', Payload=json.dumps(payload))
        logging.info("Lambda invocada")

        # Missatge de confirmacio (Ack)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# Iniciem consumer a la cua
logging.info("Consumidor iniciado. Esperando mensajes...")
canal.basic_consume(queue='InsultQueue', on_message_callback=callback)

try:
    canal.start_consuming()
except KeyboardInterrupt:
    logging.info("Interrumpido. Cerrando...")
    canal.stop_consuming()
finally:
    conexion.close()

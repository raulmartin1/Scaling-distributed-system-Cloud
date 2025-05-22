import pika
import boto3
import json

# Dades Rabbitmq
RABBITMQ_HOST = '44.201.248.130'
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password123'
QUEUE_NAME = 'InsultQueue'

# Nom de la Lamda creada a AWS
LAMBDA_FUNCTION_NAME = 'lambda_insult_filter'

# Creeem el cliente Lambda boto3
lambda_client = boto3.client('lambda', region_name='us-east-1')

def callback(ch, method, properties, body):
    mensaje = body.decode()
    print(f"Mensaje recibido: {mensaje}")
    try:
        # S'invoca la Lambda de manera asincrona
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType='Event',
            Payload=json.dumps({"mensaje_original": mensaje})
        )
        print("Lambda invocada")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error invocando Lambda: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    # Conexio a Rabbitmq i la cua
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print('Suscriptor iniciado. Esperando mensajes...')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrumpido, cerrando conexión...')
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    main()


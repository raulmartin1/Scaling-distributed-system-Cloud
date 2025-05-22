import pika

credentials = pika.PlainCredentials('user', 'password123')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='44.201.248.130', port=5672, virtual_host='/', credentials=credentials)
)
channel = connection.channel()
channel.queue_declare(queue='InsultQueue', durable=True)
print("Cua 'InsultQueue' declarada correctament.")
connection.close()

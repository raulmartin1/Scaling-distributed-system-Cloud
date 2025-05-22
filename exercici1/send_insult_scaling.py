import pika

def enviar_a_cola():
    credenciales = pika.PlainCredentials('user', 'password123')
    conexion = pika.BlockingConnection(pika.ConnectionParameters(host='44.201.248.130', port=5672, credentials=credenciales))
    canal = conexion.channel()
    canal.queue_declare(queue='InsultQueue', durable=True)

    for i in range(50):
        texto = f"Mensaje {i+1}: Eres un Tonto" if i % 2 == 0 else f"Mensaje {i+1}: Nada"
        canal.basic_publish(exchange='', routing_key='InsultQueue', body=texto, properties=pika.BasicProperties(delivery_mode=2))
        print(f"Mensaje enviado: {texto}")

    conexion.close()

if __name__ == '__main__':
    enviar_a_cola()

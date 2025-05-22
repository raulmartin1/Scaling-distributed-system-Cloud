import pika

def enviar_a_cola(texto):
    credenciales = pika.PlainCredentials('user', 'password123')
    conexion = pika.BlockingConnection(pika.ConnectionParameters(host='44.201.248.130', port=5672, credentials=credenciales))
    canal = conexion.channel()
    canal.queue_declare(queue='InsultQueue', durable=True)
    canal.basic_publish(exchange='', routing_key='InsultQueue', body=texto, properties=pika.BasicProperties(delivery_mode=2))
    print(f"Mensaje enviado: {texto}")
    conexion.close()

if __name__ == '__main__':
    textos = [
        "Este es un mensaje limpio",
        "Eres un Tonto y un Bobo",
        "Que tengas un buen día",
        "Tortuga, qué haces?"
    ]
    for texto in textos:
        enviar_a_cola(texto)

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
    insultos = ['Tonto', 'Bobo', 'Tortuga', 'Nada']
    for i in range(100):
        insulto = insultos[i % 4]
        texto = f"Mensaje {i} {insulto}"
        enviar_a_cola(texto)

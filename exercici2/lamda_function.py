import json

def lambda_handler(event, context):
    try:
        # Cogemos el mensaje
        mensaje = event.get('mensaje_original', '')
        print(f"Mensaje recibido por Lambda: {mensaje}")

        insultos = ['Tonto', 'Tortuga', 'Bobo']

        for insulto in insultos:
            # Si el insulto está en el mensaje, lo reemplazamos por 'CENSORED'
            if insulto.lower() in mensaje.lower():
                mensaje = mensaje.replace(insulto, "CENSORED")
                print(f"Insulto encontrado y reemplazado: {insulto}")

        resultado = {
            'mensaje_filtrado': mensaje,
            'estado': 'Procesado correctamente'
        }
        print(f"Mensaje filtrado: {mensaje}")

        return {
            'statusCode': 200,
            'body': json.dumps(resultado)
        }
    except Exception as e:
        print(f"Error al procesar el mensaje: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
import lithops
from lithops import FunctionExecutor
import boto3
import os
import csv
from datetime import datetime

# Configuración
BUCKET_NAME = 'nicolas-sdlab'
INPUT_PATH = 'input/'
OUTPUT_PATH = 'output/'
FORBIDDEN_WORDS = {'Tonto', 'Tortuga', 'Bobo'}
REPORT_FILE = 'censor_report.csv'

# Precomputa las palabras prohibidas en minúsculas
FORBIDDEN_WORDS_LOWER = {word.lower() for word in FORBIDDEN_WORDS}

def map_function(obj):
    """Procesa un archivo, censura palabras prohibidas y guarda en S3."""
    s3 = boto3.client('s3')
    bucket = obj.bucket
    key = obj.key
    
    # Ruta de salida (cambia 'input/' por 'output/')
    output_key = key.replace(INPUT_PATH, OUTPUT_PATH)
    temp_file = f'/tmp/censored_{os.path.basename(key)}'
    
    total_censored = 0
    processed_lines = []

    try:
        with open(temp_file, 'wb') as f_out:
            with obj.data_stream as stream:
                for line in stream:
                    line_decoded = line.decode('utf-8').strip()
                    words = line_decoded.split()
                    censored_line = []
                    for word in words:
                        cleaned_word = word.strip('.,!?()[]{}":;')
                        if cleaned_word.lower() in FORBIDDEN_WORDS_LOWER:
                            censored_line.append("CENSURADO")
                            total_censored += 1
                        else:
                            censored_line.append(word)
                    processed_lines.append(' '.join(censored_line) + '\n')
                    f_out.write((' '.join(censored_line) + '\n').encode('utf-8'))
 # Sube el archivo censurado a S3
        s3.upload_file(temp_file, bucket, output_key)
    
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    return {
        'filename': key,
        'censored_count': total_censored,
        'processed_at': datetime.now().isoformat()
    }

def reduce_function(results):
    """Suma el total de palabras censuradas y genera un reporte CSV."""
    total_censored = sum(record['censored_count'] for record in results)
    
    # Genera el reporte CSV
    with open(REPORT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Filename', 'CensoredCount', 'ProcessedAt'])
        for record in results:
            writer.writerow([
                record['filename'],
                record['censored_count'],
                record['processed_at']
            ])
    
    return total_censored

if __name__ == '__main__':
    print("Iniciando censura...")
    
    # Configura Lithops
    fexec = FunctionExecutor(
        backend='localhost'
    )
    
 # Obtiene la lista de archivos en el bucket/input/
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INPUT_PATH).get('Contents', [])
    iterdata = [f's3://{BUCKET_NAME}/{obj["Key"]}' for obj in objects if not obj["Key"].endswith('/')]
    
    # Ejecuta map-reduce
    future = fexec.map_reduce(
        map_function=map_function,
        map_iterdata=iterdata,
        reduce_function=reduce_function
    )
    
    total = fexec.get_result(future)
    print(f'Total de palabras censuradas: {total}')
    
    # Imprime el contenido del reporte
    with open(REPORT_FILE, 'r') as f:
        print("\nReporte generado:")
        print(f.read())
    
    fexec.clean()
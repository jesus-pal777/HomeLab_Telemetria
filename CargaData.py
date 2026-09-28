import boto3
import pandas as pd
import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# CONFIGURACIÓN DE MINIO / S3
MINIO_ENDPOINT = "url_bucket:9000"
ACCESS_KEY = "usuario"
SECRET_KEY = "passdowrd"
BUCKET_NAME = "nombre_bucket"

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)


""" Definimos la fecha que vamos a procesar, en este caso con CDMX """
tz_mx = ZoneInfo("America/Mexico_City")
ahora = datetime.now(tz_mx)

""" DIrección de los datos, Data Lake Bronze, Silver, Gold """"
fecha_proceso = ahora - timedelta(days=1)
fecha_str = fecha_proceso.strftime('%Y-%m-%d')  # Ejemplo de vista "2026-09-26"
anio_mes_str = fecha_proceso.strftime('%Y-%m')  # Ejemplo de vista: "2026-09"

""" Ubicación dentro del bucket """
prefix_crudos = f"crudos/{fecha_str}/"
destino_limpio = f"limpios/{anio_mes_str}/{fecha_str}.parquet" # Carpeta de histórico
destino_dia = f"Dia/{fecha_str}.parquet" # Carpeta exclusiva del día actual


try:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix_crudos)
except Exception as e:
    print(f"[ERROR] No se pudo conectar a MinIO: {e}")
    exit()

if 'Contents' in response:
    archivos_json = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
    
    """ Bucle if para crear lista vacía y posterior empezar con la validació y carga de los datos respectivamente """
    if archivos_json:
        lista_dfs = []
       

 
        """ Leer archivos json con read_json y los guardamos para hacer la unión en un solo archivo
        para evitar creación de archivos temporales se crean buffers en memoria con io.Bytes """
        for key in archivos_json:
            try:
                obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
                df_hora = pd.read_json(io.BytesIO(obj['Body'].read()))
                lista_dfs.append(df_hora)
            except Exception as e:
                print(f"No se pudo leer el archivo {key}: {e}")
        
        if lista_dfs:

            # 2 Hacemos la unión de todos los archivos json en uno solo DataFrame
            df_diario_completo = pd.concat(lista_dfs, ignore_index=True)
            
            # 3 Eliminamos duplicados y así mismo colcamos en  0 los valores nulos como velocidad o batería y cuando se tenga el estado de desconocido
            df_diario_completo = df_diario_completo.drop_duplicates()
            df_diario_completo = df_diario_completo.fillna({
                'velocidad_kmh': 0,
                'bateria': 0,
                'estado': 'desconocido'
            })
            
            # 4 Convertimos el DataFrame un formato parquet utilizando un buffer de memoria para evitar el gasto de recursos locales.
            parquet_buffer = io.BytesIO()
            df_diario_completo.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            
            # 5. Se guarda el archivo por día en un capa Silver sobre una carpeta mensual.
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=destino_limpio,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            print(f"Archivo histórico guardado en: {destino_limpio}")
            
            # 6. Archivos por día sobre carpeta única, lo que haremos será borrar cualquier archivo que exista sobre esa carpeta.
            try:
                response_dia = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="Dia/")
                if 'Contents' in response_dia:
                    objetos_a_borrar = [{'Key': obj['Key']} for obj in response_dia['Contents']]
                    s3_client.delete_objects(
                        Bucket=BUCKET_NAME,
                        Delete={'Objects': objetos_a_borrar}
                    )
                    print(f"Carpeta 'Dia' depurada (se eliminó el archivo del día anterior).")
            except Exception as e:
                print(f"No se pudo limpiar la carpeta Dia: {e}")


            
            # 7. Una vez la carpeta esté vacía se sube archivo por día en capa Golden
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=destino_dia,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            print(f"Archivo diario actual guardado en: {destino_dia}")
        else:
	# Si no se tienen datos para validar que en este caso se validó que fuesen Jsons
            print(f"No hay datos válidos para procesar en los archivos encontrados.")
    else:
	# Se tiene el bucket bien pero no hay archivos json encontrados
        print(f"No se encontraron archivos JSON en la ruta: {prefix_crudos}")
else:
	#En caso de que el bucket no sea correcto
    print(f" La ruta de crudos {prefix_crudos} no existe en MinIO.")

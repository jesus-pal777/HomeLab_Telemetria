import json
import time
import random
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import boto3
from botocore.client import Config

# CONFIGURACIÓN DE MINIO / S3
MINIO_ENDPOINT = "url_bucket_S3:9000"
ACCESS_KEY = "user"
SECRET_KEY = "password"
BUCKET_NAME = "nombre_bucket"

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

flota = [
    "BYD-DOLPHIN-001", "BYD-DOLPHIN-002", "BYD-DOLPHIN-003",
    "BYD-DOLPHIN-004", "BYD-DOLPHIN-005", "BYD-DOLPHIN-006",
    "BYD-DOLPHIN-007", "BYD-DOLPHIN-008", "BYD-DOLPHIN-009", "BYD-DOLPHIN-010"
]

estado_flota = {}

def inicializar_flota():
    """Recorre toda nuestra lista de autos y le asigna valores de inicio aleatorios"""
    for vehiculo in flota:
        estado_flota[vehiculo] = {
            "latitud": round(random.uniform(19.0480, 19.5930), 6),
            "longitud": round(random.uniform(-99.3650, -98.9400), 6),
            "bateria": random.randint(25, 100),
            "estado": random.choice(["disponible", "en_viaje", "recargando", "fuera"])
        }

def actualizar_vehiculo(vehiculo_id):
    """Modifica la posición (coordenadas), simula el consumo de bateria y la velocidad de los autos"""
    coche = estado_flota[vehiculo_id]
    
    delta_lat = random.uniform(-0.0002, 0.0002)
    delta_lon = random.uniform(-0.0002, 0.0002)
    
    coche["latitud"] = round(coche["latitud"] + delta_lat, 6)
    coche["longitud"] = round(coche["longitud"] + delta_lon, 6)
    
    if coche["estado"] == "en_viaje":
        velocidad = random.randint(15, 68)
        if random.random() < 0.10:
            coche["bateria"] = max(5, coche["bateria"] - 1)
        if coche["bateria"] < 15:
            coche["estado"] = "recargando"
    elif coche["estado"] == "recargando":
        velocidad = 0
        coche["bateria"] = min(100, coche["bateria"] + 5)
        if coche["bateria"] == 100:
            coche["estado"] = "disponible"
    else:
        velocidad = 0
        if random.random() < 0.20:
            coche["estado"] = "en_viaje"
            
    evento = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "vehiculo_id": vehiculo_id,
        "latitud": coche["latitud"],
        "longitud": coche["longitud"],
        "velocidad_kmh": velocidad,
        "bateria": coche["bateria"],
        "estado": coche["estado"]
    }
    return evento

def iniciar_streaming():
    """Bucle que genera eventos cada 2 segundos y respalda en MinIO exactamente al llegar al minuto 50."""
    inicializar_flota()
    buffer_eventos = []
    guardado_esta_hora = False
    
    
    while True:
        vehiculo_elegido = random.choice(flota)
        nuevo_evento = actualizar_vehiculo(vehiculo_elegido)
        print(json.dumps(nuevo_evento))
        
        buffer_eventos.append(nuevo_evento)
        
        """ Obtenemos la hora actual en CDMX"""
        now_local = datetime.now(ZoneInfo("America/Mexico_City"))
        
        """ Se guarda el archivo en MinIO en el minuto 50 de cada hora """
        if now_local.minute == 50 and not guardado_esta_hora:
            if len(buffer_eventos) > 0:
                fecha_str = now_local.strftime("%Y-%m-%d")
                timestamp_str = now_local.strftime("%H%M%S")
                
                file_name = f"Crudos/{fecha_str}/flota_batch_{timestamp_str}.json"
                
                try:
                    s3_client.put_object(
                        Bucket=BUCKET_NAME,
                        Key=file_name,
                        Body=json.dumps(buffer_eventos, indent=2).encode('utf-8'),
                        ContentType='application/json'
                    )
                    print(f"[EXITO] Respaldo de hora guardado en MinIO (Minuto 50) -> {file_name}")
                except Exception as e:
                    print(f"[ERROR] No se pudo guardar en MinIO: {e}")
                    
            buffer_eventos = []
            guardado_esta_hora = True
            
        # Se reinicia el reloj para que se guarde cuando vuelvan a ser la hora y 50 min.
        elif now_local.minute != 50:
            guardado_esta_hora = False
            
        time.sleep(2)

if __name__ == "__main__":
    iniciar_streaming()
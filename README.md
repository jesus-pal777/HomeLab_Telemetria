# HomeLab Telemetría: End-to-End Data Lake & ETL Pipeline

Proyecto End-to-End sobre un Data Lake con **Arquitectura Medallion**, diseñado para la ingesta, procesamiento y almacenamiento estructurado de telemetría en tiempo real de una flota de vehículos eléctricos en la CDMX.


## Arquitectura y Capas de Datos en Data Lake
* **Capa Bronze (Cruda):** Ingesta y almacenamiento de eventos en formato JSON (generado por el script simulador de streaming) otorgando persistencia de los datos originales (crudos).
* **Capa Silver (Procesada):** Transformación, limpieza y estructuración de los datos para mantener un registro histórico.
* **Optimización (Parquet):** Compactación de la información en archivos particionados por día en formato columnar **Parquet** esto pensando en la optimización del espacio y aceleración de consultas y para un fácil consumo de los datos así como para un análisis más profundo en caso de requerirse.


## Tecnologías Utilizadas
* **Lenguajes:** Python (Boto3, Datetime, Random, ZoneInfo, Pandas)
* **Almacenamiento (Data Lake):** MinIO (S3 Compatible Object Storage)
* **Formatos de Datos:** JSON, Parquet
* **Infraestructura y OS:** Docker, Linux Mint 22.3 (HomeLab)


## Flujo de Datos

1. **Simulación de Flota (`SimuladorAutos.py`):**
   * Se tiene una flota 10 vehículos con coordenadas geográficas reales de la CDMX.
   * Simula la conducción de los vehiculos (desplazamientos de coordenadas, variaciones de velocidad , consumo y recarga de batería).
   * Genera eventos cada 2 segundos.

2. **Carga por Lotes (*Batch Ingestion*):**
   * Cuenta con un disparador (*trigger*) sincronizado al **minuto 50** de cada hora(`America/Mexico_City`) para realizar el guardado de datos Crudos.
   * Empaqueta el lote acumulado y lo carga automáticamente al bucket de MinIO por particiones de fecha, teniendo el siguiente formato: `Crudos/YYYY-MM-DD/flota_batch_HHMMSS.json`
   * Se realiza la unión de los archivos por hora y se guardan en la siguiente capa del Data Lake con el siguiente formato: `Limpios/YYYY-MM/YYYY-MM-DD.parquet`. El guardado se hace por día
   * Cuenta con una última capa (Gold) en donde solo se tiene 1 archivo (del día anterior) optimizado para que se borre y solo se tenga 1 archivo del último corte


## Estructura del Proyecto

```text
HomeLab_Telemetria/
├── SimuladorAutos.py    # Script principal de simulación y conexión con MinIO para carga de datos Crudos
└── CargaData.py         # Script para limpieza, y agregaciión a capas Silver y Gold de nuestro Data Lake

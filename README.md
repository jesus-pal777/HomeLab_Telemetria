# HomeLab Telemetría: End-to-End Data Lake & ETL Pipeline

Proyecto End-to-End sobre un Data Lake con **Arquitectura Medallion**, diseñado para la ingesta, procesamiento y almacenamiento estructurado de telemetría en tiempo real de una flota de vehículos eléctricos en la CDMX.

## Arquitectura y Capas de Datos
* **Capa Bronze (Cruda):** Ingesta y almacenamiento de eventos en formato JSON tal como llegan del simulador de streaming, garantizando la persistencia de los datos originales.
* **Capa Silver (Procesada):** Transformación, limpieza y estructuración de los datos para mantener un registro histórico orientado a auditorías y consultas analíticas.
* **Optimización (Parquet):** Compactación de la información en archivos particionados por día en formato columnar **Parquet**, optimizando drásticamente el espacio de almacenamiento y acelerando las consultas del corte diario.

## Tecnologías Utilizadas
* **Lenguajes:** Python (Boto3, Datetime, Random, ZoneInfo)
* **Almacenamiento (Data Lake):** MinIO (S3 Compatible Object Storage)
* **Formatos de Datos:** JSON, Parquet
* **Infraestructura y OS:** Docker, Linux Mint 22.3   (HomeLab)
* **Control de Versiones:** GitHub


## Flujo de Datos

1. **Simulación de Flota (`SimuladorAutos.py`):**
   * Se tiene una flota 10 vehículos con coordenadas geográficas reales de la CDMX.
   * Simula dinámicas de conducción (desplazamientos con deltas de coordenadas, variaciones de velocidad entre 15 y 68 km/h, consumo y recarga de batería).
   * Genera eventos de telemetría cada 2 segundos con marcas de tiempo universales (`UTC`).

2. **Carga por Lotes (*Batch Ingestion*):**
   * Almacena los eventos en un buffer en memoria durante la hora de operación.
   * Cuenta con un disparador (*trigger*) sincronizado exactamente al **minuto 50** de cada hora (`America/Mexico_City`).
   * Empaqueta el lote acumulado y lo carga automáticamente al bucket de MinIO bajo una estructura optimizada por particiones de fecha:
     `Crudos/YYYY-MM-DD/flota_batch_HHMMSS.json`


## Estructura del Repositorio

```text
HomeLab_Telemetria/
├── SimuladorAutos.py    # Script principal de simulación y carga a MinIO
├── .gitignore           # Archivos excluidos por seguridad y rendimiento
└── README.md            # Documentación del proyecto

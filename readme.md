# Dual API ETL Pipeline

## Descripción
Este proyecto implementa un pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow para procesar datos de dos APIs diferentes y cargarlos en una base de datos Redshift.

## Características
- Extracción de datos de dos APIs distintas
- Transformación y combinación de los datos extraídos
- Carga de datos en una base de datos Redshift
- Programación diaria del pipeline
- Sistema de alertas por correo electrónico en caso de fallos

## Requisitos
- Python 3.x
- Apache Airflow
- Bibliotecas Python: (lista de bibliotecas necesarias)
- Cuenta de Amazon Redshift

## Configuración
1. Clonar el repositorio
2. Instalar las dependencias: `pip install -r requirements.txt`
3. Configurar las variables de entorno en un archivo `.env`:
   ```
      REDSHIFT_HOST=XXXXXXXXXXXXXXXXXXXXXXXx
      REDSHIFT_USER=XXXXXXXXXXXXXXXXXXXXXXXx
      REDSHIFT_PASS=XXXXXXXXXXXXXXXXXXXXXXXx
      REDSHIFT_DB=XXXXXXXXXXXXXXXXXXXXXXXx
      REDSHIFT_PORT=XXXXXXXXXXXXXXXXXXXXXXXx

      AIRFLOW_UID=1000
      ALPHA_VANTAGE_API_KEY=XXXXXXXXXXXXXXXXXXXXXXXx
      NEWS_API_KEY=XXXXXXXXXXXXXXXXXXXXXXXx


   ```
4. Configurar las credenciales de las APIs (si es necesario)

## Estructura del proyecto
```
project/
│
├── dags/
│   └── dual_api_etl_dag.py
│
├── raw_data/
│
├── processed_data/
│
├── .env
├── requirements.txt
└── README.md
```

## Uso
1. Iniciar el scheduler de Airflow
2. El DAG `Dual_API_ETL` se ejecutará diariamente de forma automática
3. Monitorear la ejecución a través de la interfaz web de Airflow

## Flujo de trabajo
1. Extracción de datos de la API 1
2. Extracción de datos de la API 2
3. Transformación y combinación de los datos
4. Conexión a Redshift
5. Carga de datos en Redshift

## Manejo de errores
- En caso de fallo en cualquier tarea, se enviará una alerta por correo electrónico

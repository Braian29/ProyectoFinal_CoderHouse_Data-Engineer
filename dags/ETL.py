from datetime import timedelta, datetime
from pathlib import Path
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import os

# Importar las funciones de tu ETL
from only_etl import extraer_datos_acciones, extraer_noticias, transformar_datos, cargar_datos

load_dotenv()  # toma las variables de entorno de .env

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Braian',
    'start_date': datetime(2024, 6, 25),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: enviar_alerta(context)
}

etl_dag = DAG(
    dag_id='Nuevo_ETL',
    default_args=default_args,
    description='ETL diario para datos de acciones y noticias',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()  # Path original.. home en Docker

# Función de envío de alerta por correo
def enviar_alerta(context):
    email = EmailOperator(
        task_id='send_email',
        to='braianalonso29@gmail.com',
        subject='Airflow Task Failed',
        html_content=f"""
        <h3>La tarea {context['task_instance_key_str']} ha fallado.</h3>
        <p>Dag: {context['task_instance'].dag_id}</p>
        <p>Tarea: {context['task_instance'].task_id}</p>
        <p>Ejecutada en: {context['execution_date']}</p>
        """,
        dag=etl_dag
    )
    return email.execute(context)

# Tareas
# 1. Extracción de datos de acciones
task_1 = PythonOperator(
    task_id='extraer_datos_acciones',
    python_callable=extraer_datos_acciones,
    op_kwargs={'exec_date': '{{ ds }} {{ execution_date.hour }}'},
    dag=etl_dag,
)

# 2. Extracción de noticias
task_2 = PythonOperator(
    task_id='extraer_noticias',
    python_callable=extraer_noticias,
    op_kwargs={'exec_date': '{{ ds }} {{ execution_date.hour }}'},
    dag=etl_dag,
)

# 3. Transformación de datos
task_3 = PythonOperator(
    task_id='transformar_datos',
    python_callable=transformar_datos,
    op_kwargs={'exec_date': '{{ ds }} {{ execution_date.hour }}'},
    dag=etl_dag,
)

# 4. Carga de datos
task_4 = PythonOperator(
    task_id='cargar_datos',
    python_callable=cargar_datos,
    op_kwargs={'exec_date': '{{ ds }} {{ execution_date.hour }}'},
    dag=etl_dag,
)

# Definir dependencias entre tareas
[task_1, task_2] >> task_3 >> task_4

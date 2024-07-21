import os
from datetime import datetime
from only_etl import extraer_datos_acciones, extraer_noticias, transformar_datos, cargar_datos

def test_etl():
    # Configurar la fecha de ejecución
    exec_date = datetime.now()

    # Crear directorios si no existen
    os.makedirs("raw_data", exist_ok=True)
    os.makedirs("processed_data", exist_ok=True)

    # Ejecutar cada paso del ETL
    print("Extrayendo datos de acciones...")
    extraer_datos_acciones(exec_date=exec_date)

    print("Extrayendo noticias...")
    extraer_noticias(exec_date=exec_date)

    print("Transformando datos...")
    transformar_datos(exec_date=exec_date)

    print("Cargando datos en Redshift...")
    cargar_datos(exec_date=exec_date)

    print("ETL completado.")

if __name__ == "__main__":
    test_etl()
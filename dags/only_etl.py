import httpx
import json
import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()  # Cargar variables de entorno desde .env

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

dag_path = os.getcwd()

def extraer_datos_acciones(symbol='TSLA', exec_date=None):
    if exec_date is None:
        exec_date = datetime.now()
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
    try:
        response = httpx.get(url)
        response.raise_for_status()
        data = response.json()
        with open(f"{dag_path}/raw_data/stock_data_{symbol}_{exec_date.date()}.json", "w") as file:
            json.dump(data, file)
        print(f"Datos de acciones guardados para {symbol}")
    except httpx.HTTPStatusError as e:
        print(f"Error HTTP al obtener datos de acciones: {e}")
    except Exception as e:
        print(f"Error al obtener datos de acciones: {e}")

def extraer_noticias(query='Tesla', exec_date=None):
    if exec_date is None:
        exec_date = datetime.now()
    url = f"https://newsapi.org/v2/everything?q={query}&apiKey={NEWS_API_KEY}"
    try:
        response = httpx.get(url)
        response.raise_for_status()
        data = response.json()
        with open(f"{dag_path}/raw_data/news_data_{query}_{exec_date.date()}.json", "w") as file:
            json.dump(data, file)
        print(f"Noticias guardadas para {query}")
    except httpx.HTTPStatusError as e:
        print(f"Error HTTP al obtener noticias: {e}")
    except Exception as e:
        print(f"Error al obtener noticias: {e}")

def clasificar_sentimiento(titulo):
    palabras_positivas = ['subida', 'aumento', 'éxito', 'innovación']
    palabras_negativas = ['caída', 'problema', 'fracaso', 'riesgo']
    
    if any(palabra in titulo.lower() for palabra in palabras_positivas):
        return 'Positivo'
    elif any(palabra in titulo.lower() for palabra in palabras_negativas):
        return 'Negativo'
    else:
        return 'Neutral'

def transformar_datos(exec_date=None):
    if exec_date is None:
        exec_date = datetime.now()
    try:
        with open(f"{dag_path}/raw_data/stock_data_TSLA_{exec_date.date()}.json", "r") as file:
            stock_data = json.load(file)
        
        with open(f"{dag_path}/raw_data/news_data_Tesla_{exec_date.date()}.json", "r") as file:
            news_data = json.load(file)
        
        df_stock = pd.DataFrame(stock_data['Time Series (Daily)']).T
        df_stock = df_stock.reset_index().rename(columns={'index': 'date'})
        
        df_news = pd.DataFrame(news_data['articles'])
        
        df_news['sentimiento'] = df_news['title'].apply(clasificar_sentimiento)
        
        df_combined = pd.merge(df_stock, df_news, left_on='date', right_on='publishedAt', how='left')
        
        df_combined.to_csv(f"{dag_path}/processed_data/combined_data_{exec_date.date()}.csv", index=False)
        print("Datos transformados y guardados")
    except Exception as e:
        print(f"Error al transformar datos: {e}")


def cargar_datos(exec_date=None):
    if exec_date is None:
        exec_date = datetime.now()
    try:
        df = pd.read_csv(f"{dag_path}/processed_data/combined_data_{exec_date.date()}.csv")
        
        conn = psycopg2.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT"),
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASS")
        )
        
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tesla_stock_news (
                date DATE,
                "open" FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT,
                title TEXT,
                description TEXT,
                sentimiento TEXT
            )
        """)
        
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO tesla_stock_news (date, "open", high, low, close, volume, title, description, sentimiento)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['date'], row['1. open'], row['2. high'], row['3. low'], row['4. close'], row['5. volume'],
                  row['title'], row['description'], row['sentimiento']))
        
        conn.commit()
        cur.close()
        conn.close()
        print("Datos cargados en Redshift")
    except Exception as e:
        print(f"Error al cargar datos: {e}")

if __name__ == "__main__":
    exec_date = datetime.now()
    extraer_datos_acciones(exec_date=exec_date)
    extraer_noticias(exec_date=exec_date)
    transformar_datos(exec_date=exec_date)
    cargar_datos(exec_date=exec_date)
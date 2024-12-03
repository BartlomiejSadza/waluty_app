import requests
import pandas as pd
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
import os

# Wczytanie zmiennych środowiskowych z pliku .env
load_dotenv()

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TABLE = os.getenv("TABLE")


# Funkcja do pobierania danych o kryptowalutach USDT
def pobierz_top30_usdt():
    url = 'https://api.binance.com/api/v3/ticker/price'
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Błąd w pobieraniu danych: {response.status_code}")
        return None

    ceny = response.json()

    # Filtrowanie par tylko z USDT
    lista = []
    for waluta in ceny:
        if waluta["symbol"].endswith("USDT"):
            symbol = waluta["symbol"]
            cena = float(waluta["price"])
            lista.append({"symbol": symbol, "cena": cena, "data": datetime.now()})

    # Pobieramy tylko top 30 par i resetujemy indeksy
    df = pd.DataFrame(lista).sort_values(by="cena", ascending=False).head(30).reset_index(drop=True)
    return df


# Pivot na dane z szerokimi kolumnami
def przygotuj_dane_pivot(df):
    # Grupowanie i pivotowanie
    df_grouped = df.groupby(['data', 'symbol'])['cena'].mean().unstack().reset_index()
    return df_grouped


# Zapis do SQL
def wrzuc_do_sql(df):
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={DB_USERNAME};"
        f"PWD={DB_PASSWORD}"
    )
    cursor = conn.cursor()

    # Przygotowanie tabeli (jednorazowo, jeśli jeszcze jej nie ma)
    columns = ', '.join([f"[{col}] FLOAT" for col in df.columns if col != 'data'])
    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{TABLE}' AND xtype='U')
    CREATE TABLE {TABLE} (
        data DATETIME,
        {columns}
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Wstawianie danych
    for _, row in df.iterrows():
        values = ', '.join([str(row[col]) if pd.notna(row[col]) else 'NULL' for col in df.columns])
        insert_query = f"INSERT INTO {TABLE} VALUES ({values})"
        cursor.execute(insert_query)
    conn.commit()
    conn.close()


# Główna funkcja
if __name__ == "__main__":
    dane = pobierz_top30_usdt()
    if dane is not None:
        pivot_dane = przygotuj_dane_pivot(dane)
        print("Przygotowane dane do SQL:")
        print(pivot_dane)
        wrzuc_do_sql(pivot_dane)
        print("Dane zapisane do SQL!")
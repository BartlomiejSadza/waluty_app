import os
import logging
import requests
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
import time

logging.basicConfig(
    level=logging.INFO,  # Zmień na DEBUG, jeśli chcesz więcej szczegółów
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# Wczytaj dane z pliku .env
load_dotenv()

# Pobranie danych z pliku .env
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")
TABLE = os.getenv("TABLE")
API_URL = os.getenv("API_URL")

def pobierz_kursy(api_url, target_currencies):
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"Błąd API: {response.status_code}")
    dane = response.json()
    if "conversion_rates" not in dane:
        raise ValueError("Brak danych 'conversion_rates' w odpowiedzi API")
    return {waluta: dane["conversion_rates"].get(waluta) for waluta in target_currencies}

def zapisz_do_sql(server, database, username, password, tabela, dane):
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    timestamp = datetime.now()
    kolumny = ", ".join(dane.keys())
    wartosci = ", ".join(["?" for _ in dane])
    zapytanie = f"INSERT INTO {tabela} (timestamp, {kolumny}) VALUES (?, {wartosci})"
    cursor.execute(zapytanie, [timestamp] + list(dane.values()))
    conn.commit()
    conn.close()

def zapisz_do_sql_with_retry(server, database, username, password, tabela, dane, retries=3, delay=5):
    for attempt in range(retries):
        try:
            zapisz_do_sql(server, database, username, password, tabela, dane)
            logging.info(f"Dane zapisane do SQL po {attempt + 1} próbie/ach.")
            return
        except Exception as e:
            logging.error(f"Próba {attempt + 1} nieudana: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logging.error("Wszystkie próby zapisu nie powiodły się.")
                raise e

def main(mytimer):
    try:
        target_currencies = ["USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "NZD"]
        kursy = pobierz_kursy(API_URL, target_currencies)
        zapisz_do_sql_with_retry(SERVER, DATABASE, USERNAME, PASSWORD, TABLE, kursy)
        logging.info(f"Kursy zapisane do SQL o {datetime.now()}")
    except Exception as e:
        logging.error(f"Błąd w funkcji głównej: {e}")


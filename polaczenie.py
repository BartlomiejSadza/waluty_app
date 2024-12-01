import os
import requests
import pyodbc
from datetime import datetime
from dotenv import load_dotenv

# Wczytaj dane z pliku .env
load_dotenv()

# Pobranie danych z pliku .env
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
TABLE = os.getenv("TABLE")
API = os.getenv("API")


# Funkcja do pobierania kursów walut z ExchangeRate API
def pobierz_kursy(api_url, target_currencies):
    """
    Pobiera kursy walut z ExchangeRate API.

    Args:
        api_url (str): URL API z kluczem.
        target_currencies (list): Lista walut docelowych.

    Returns:
        dict: Słownik z kursami walut.
    """
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"Błąd API: {response.status_code}")

    dane = response.json()
    if "conversion_rates" not in dane:
        raise ValueError("Brak danych 'conversion_rates' w odpowiedzi API")

    # Filtrowanie walut
    kursy = {waluta: dane["conversion_rates"].get(waluta) for waluta in target_currencies}
    return kursy


# Funkcja do przebudowy tabeli
def przebuduj_tabele(server, database, username, password, tabela, waluty):
    """
    Tworzy nową tabelę z kolumnami dla podanych walut.

    Args:
        server (str): Adres serwera Azure SQL.
        database (str): Nazwa bazy danych.
        username (str): Login do SQL.
        password (str): Hasło do SQL.
        tabela (str): Nazwa tabeli docelowej.
        waluty (list): Lista walut do stworzenia kolumn.
    """
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Usunięcie tabeli, jeśli istnieje
    cursor.execute(f"IF OBJECT_ID('{tabela}', 'U') IS NOT NULL DROP TABLE {tabela}")
    conn.commit()

    # Tworzenie nowej tabeli
    kolumny = ", ".join([f"[{waluta}] FLOAT" for waluta in waluty])
    cursor.execute(f"""
    CREATE TABLE {tabela} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        timestamp DATETIME,
        {kolumny}
    )
    """)
    conn.commit()
    conn.close()
    print(f"Tabela {tabela} przebudowana z kolumnami: {', '.join(waluty)}")


# Funkcja do zapisywania danych do SQL
def zapisz_do_sql(server, database, username, password, tabela, dane):
    """
    Zapisuje dane do nowej struktury tabeli.

    Args:
        server (str): Adres serwera Azure SQL.
        database (str): Nazwa bazy danych.
        username (str): Login do SQL.
        password (str): Hasło do SQL.
        tabela (str): Nazwa tabeli docelowej.
        dane (dict): Dane do zapisania (kursy walut).
    """
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Wstawianie danych
    timestamp = datetime.now()
    kolumny = ", ".join(dane.keys())
    wartosci = ", ".join(["?" for _ in dane])
    zapytanie = f"INSERT INTO {tabela} (timestamp, {kolumny}) VALUES (?, {wartosci})"
    cursor.execute(zapytanie, [timestamp] + list(dane.values()))
    conn.commit()
    conn.close()
    print(f"Kursy zapisane do SQL o {timestamp}")


if __name__ == "__main__":
    # Twój URL API z kluczem
    api_url = f"https://v6.exchangerate-api.com/v6/{API}/latest/PLN"
    target_currencies = ["USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "CNY", "HKD", "NZD"]

    try:
        # Przebudowa tabeli (tylko raz)
        # przebuduj_tabele(SERVER, DATABASE, USERNAME, PASSWORD, TABLE, target_currencies)

        # Pobieranie kursów
        kursy = pobierz_kursy(api_url, target_currencies)
        print(f"Pobrano kursy: {kursy}")

        # Zapis do SQL
        zapisz_do_sql(SERVER, DATABASE, USERNAME, PASSWORD, TABLE, kursy)
    except Exception as e:
        print(f"Błąd: {e}")
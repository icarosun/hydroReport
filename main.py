import requests 
from datetime import datetime, time, timezone, UTC
import sqlite3
from concurrent.futures import ThreadPoolExecutor

URL = "https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/" 

watersheds_to_insert = [
    (1, "RIO AMAZONAS"),
    (2, "RIO MADEIRA",),
    (3, "RIO NEGRO",),
    (4, "RIO PURUS",),
    (5, "RIO SOLIMÕES",)
]

station_to_insert = [
    (1, 1, "15030000", "Jaturana (Alto Rio Amazonas)", 1920, "15/08/2025", None, None, 1920, "01/06/2021", 179, "10/10/2024"),
    (2, 1, "16030000", "Itacoatiara (Médio Rio Amazonas)", 1310, "18/08/2025", None, None, 1520, "27/05/2021", -14, "13/10/2024"),
    (3, 1, "17050001", "Óbidos (Baixo Rio Amazonas)", 676, "19/08/2025", None, None, 860, "31/05/2009", -130, "12/10/2024"),
    (4, 2, "15400000", "Porto Velho (Alto Rio Madeira)", 445, "19/08/2025", None, None, 1966, "27/03/2014", 179, "20/09/2024"),
    (5, 2, "15630000", "Humaitá (Médio Rio Madeira)", 1247, "19/08/2025", None, None, 2563, "09/04/2014", 8, "16/10/2024"),
    (6, 2, "15700000", "Manicoré (Baixo Rio Madeira)", 1553, "18/08/2025", None, None, 2887, "19/04/2014", 542, "01/10/1969"),
    (7, 3, "14480002", "Barcelos (Alto Rio Negro)", 875, "18/08/2025", None, None, 1052, "22/06/2022", 58, "18/03/1980"),
    (8, 3, "14840000", "Moura (Médio Rio Negro)", 1393, "18/08/2025", None, None, 1598, "21/06/2022", 225, "17/11/2023"),
    (9, 3, "14990000", "Manaus (Baixo Rio Negro)", 2761, "19/08/2025", None, None, 3002, "16/06/2021", 1213, "02/11/2024"),
    (10, 4, "13710001", "Valparaíso (Alto Rio Purus)", 340, "18/08/2025", None, None, 2026, "04/03/2021", 248, "02/10/2024"),
    (11, 4, "13870000", "Labrea (Médio Rio Purus)", 571, "19/08/2025", None, None, 2179, "13/04/1997", 45, "17/10/1937"),
    (12, 4, "13990000", "Beruri (Baixo Rio Purus)", 1936, "19/08/2025", None, None, 2236, "24/06/2015", 254, "11/10/2024"),
    (13, 5, "10100000", "Tabatinga (Alto Rio Solimões)", 522, "19/08/2025", None, None, 1382, "28/05/1999", -254, "26/09/2024"),
    (14, 5, "13150000", "Coari (Médio Rio Solimões)", 1574, "11/08/2025", None, None, 1801, "24/06/2015", -29, "09/10/2024"),
    (15, 5, "14100000", "Manacapuru (Baixo Rio Solimões)", 1837, "19/08/2025", None, None, 2086, "17/06/2021", 311, "26/10/2023"),
]

stations = {
    "15030000": "https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15030000",
    '16030000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/16030000',
    '17050001' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/17050001',
    '15400000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15400000',
    '15630000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15630000',
    '15700000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15700000',
    '14480002' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14480002',
    '14840000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14840000',
    '14990000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14990000',
    '13710001' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13710001',
    '13870000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13870000',
    '13990000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13990000',
    '10100000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/10100000',
    '13150000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13150000',
    '14100000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14100000',
} 

today_7h_utc = datetime.combine(datetime.now(UTC).date(), time(7, 0, 0), tzinfo=timezone.utc)

parser_today_7h_utc = today_7h_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

# for water in watersheds:
#     for station in watersheds[water]:
#         response = requests.get(URL + station)
#
#         if response.status_code == 200:
#             print("OK")
#             parser_json = (response.json())
#             datas = parser_json["items"]
#             for data in datas:
#                 if data['data'] == parser_today_7h_utc:
#                     print(parser_today_7h_utc)
#                     print(data['nivel']) 
#
def conect_database():
    conn = sqlite3.connect('database.db')
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = conect_database()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS watershed (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS station (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            watershed_id INTEGER,
            cod_station TEXT UNIQUE NOT NULL,
            name TEXT,
            current_level INTEGER,
            current_level_data TEXT,
            last_level INTEGER,
            last_level_data TEXT,
            max_record INTEGER,
            data_max_record TEXT,
            min_record INTEGER,
            data_min_record TEXT,
            FOREIGN KEY (watershed_id) REFERENCES watershed(id)
        )
    ''')

    conn.commit()
    conn.close()

def is_database_populated():
    try:
        conn = conect_database()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM watershed")

        count = cursor.fetchone()[0]

        return count > 0

    except sqlite3.Error as e:
        print(f"Error ao verificar o banco: {e}")
        return False
    finally:
        if conn:
            conn.close()

def populate_database():
    try:
        conn = conect_database()
        cursor = conn.cursor()

        cursor.executemany("INSERT INTO watershed VALUES (?, ?)", watersheds_to_insert)
        print(f"Inserted {cursor.rowcount} rows successfully")

        cursor.executemany("INSERT INTO station VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", station_to_insert)
        print(f"Inserted {cursor.rowcount} rows successfully")
        
        conn.commit()

    except sqlite3.Error as e:
        print(f"An error ocurred: {e}")
    finally:
        if conn:
            conn.close()

def main():
    init_db()

    if not is_database_populated():
        populate_database()

    for cod_station, url in stations.items():
        print(cod_station)
 
    

if __name__ == "__main__":
    main()

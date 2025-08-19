import requests 
from datetime import datetime, time, timezone, UTC
from concurrent.futures import ThreadPoolExecutor

URL = "https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/" 

watersheds = {
    "RIO AMAZONAS": ["15030000"]
}

today_7h_utc = datetime.combine(datetime.now(UTC).date(), time(7, 0, 0), tzinfo=timezone.utc)

parser_today_7h_utc = today_7h_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

for water in watersheds:
    for station in watersheds[water]:
        response = requests.get(URL + station)

        if response.status_code == 200:
            print("OK")
            parser_json = (response.json())
            datas = parser_json["items"]
            for data in datas:
                if data['data'] == parser_today_7h_utc:
                    print(parser_today_7h_utc)
                    print(data['nivel']) 

def init_db():
    conn = conect_database()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS watershed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS station (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_station TEXT,
            name TEXT,
            current_level INTEGER,
            current_level_data TEXT,
            last_level INTEGER,
            last_level_data TEXT,
            max_record TEXT,
            data_max_record TEXT,
            min_record TEXT,
            data_min_record TEXT,
        )
    ''')

    conn.commit()
    conn.close()


def main():
    init_db()


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
    (1, 1, '15030000', 'Jaturana (Alto Rio Amazonas)', 1705, '2025-08-15T00:00:00Z', None, None, 1920, '2021-06-01T00:00:00Z', 179, '2024-10-10T00:00:00Z'),
    (2, 1, '16030000', 'Itacoatiara (Médio Rio Amazonas)', 1289, '2025-08-21T00:00:00Z', None, None, 1520, '2021-05-27T00:00:00Z', -14, '2024-10-13T00:00:00Z'),
    (3, 1, '17050001', 'Óbidos (Baixo Rio Amazonas)', 668, '2025-08-21T00:00:00Z', None, None, 860, '2009-05-31T00:00:00Z', -130, '2024-10-12T00:00:00Z'),
    (4, 2, '15400000', 'Porto Velho (Alto Rio Madeira)', 394, '2025-08-21T00:00:00Z', None, None, 1966, '2014-03-27T00:00:00Z', 179, '2024-09-20T00:00:00Z'),
    (5, 2, '15630000', 'Humaitá (Médio Rio Madeira)', 1209, '2025-08-21T00:00:00Z', None, None, 2563, '2014-04-09T00:00:00Z', 8, '2024-10-16T00:00:00Z'),
    (6, 2, '15700000', 'Manicoré (Baixo Rio Madeira)', 1553, '2025-08-18T00:00:00Z', None, None, 2887, '2014-04-19T00:00:00Z', 542, '1969-10-01T00:00:00Z'),
    (7, 3, '14480002', 'Barcelos (Alto Rio Negro)', 875, '2025-08-18T00:00:00Z', None, None, 1052, '2022-06-22T00:00:00Z', 58, '1980-03-18T00:00:00Z'),
    (8, 3, '14840000', 'Moura (Médio Rio Negro)', 1377, '2025-08-20T00:00:00Z', None, None, 1598, '2022-06-21T00:00:00Z', 225, '2023-11-17T00:00:00Z'),
    (9, 3, '14990000', 'Manaus (Baixo Rio Negro)', 2748, '2025-08-21T00:00:00Z', None, None, 3002, '2021-06-16T00:00:00Z', 1213, '2024-11-02T00:00:00Z'),
    (10, 4, '13710001', 'Valparaíso (Alto Rio Purus)', 340, '2025-08-18T00:00:00Z', None, None, 2026, '2021-03-04T00:00:00Z', 248, '2024-10-02T00:00:00Z'),
    (11, 4, '13870000', 'Labrea (Médio Rio Purus)', 565, '2025-08-20T00:00:00Z', None, None, 2179, '1997-04-13T00:00:00Z', 45, '1937-10-17T00:00:00Z'),
    (12, 4, '13990000', 'Beruri (Baixo Rio Purus)', 1920, '2025-08-21T00:00:00Z', None, None, 2236, '2015-06-24T00:00:00Z', 254, '2024-10-11T00:00:00Z'),
    (13, 5, '10100000', 'Tabatinga (Alto Rio Solimões)', 532, '2025-08-21T00:00:00Z', None, None, 1382, '1999-05-28T00:00:00Z', -254, '2024-09-26T00:00:00Z'),
    (14, 5, '13150000', 'Coari (Médio Rio Solimões)', 1532, '2025-08-18T00:00:00Z', None, None, 1801, '2015-06-24T00:00:00Z', -29, '2024-10-09T00:00:00Z'),
    (15, 5, '14100000', 'Manacapuru (Baixo Rio Solimões)', 1823, '2025-08-21T00:00:00Z', None, None, 2086, '2021-06-17T00:00:00Z', 311, '2023-10-26T00:00:00Z')
]

stations = {
    "15030000": "https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15030000",
    '16030000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/16030000',
    '17050001' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/17050001',
    # '15400000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15400000',
    # '15630000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15630000',
    # '15700000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/15700000',
    # '14480002' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14480002',
    # '14840000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14840000',
    # '14990000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14990000',
    # '13710001' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13710001',
    # '13870000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13870000',
    # '13990000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13990000',
    # '10100000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/10100000',
    # '13150000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/13150000',
    # '14100000' : 'https://ows.snirh.gov.br/ords/servicos/hidro/estacao/24h/14100000',
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

def get_info_station(station):
    conn = conect_database()
    cursor = conn.cursor()

    cursor.execute("SELECT id, current_level, current_level_data FROM station WHERE cod_station = ?", (station,))

    result = cursor.fetchone()

    if result:
        conn.close()

        return result

def update_station(current_cota, new_cota):
    conn = conect_database()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE station SET current_level = ?, current_level_data = ?, last_level = ?, last_level_data = ? WHERE id = ?", 
        (new_cota['nivel'], new_cota['data'], current_cota[1], current_cota[2], current_cota[0],)
    )

    conn.commit()
    conn.close()

def update_all_station(cod_station, link):
    try:
        print(f"🔍 Buscando dados para {cod_station}...")
        response = requests.get(link)
        dados = response.json()

        for cota in dados['items']:
            if (cota['data'] == parser_today_7h_utc):
                if (cota['nivel'] != None):
                    current_station = get_info_station(cod_station) 
                    update_station(current_station, cota)
                    
                    print(f"✅ {cod_station} salvo com sucesso.")
                return

    except Exception as e:
        print(f"❌ Erro ao processar {cod_station}: {e}")

def generate_report():
    conn = conect_database()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM watershed")

    all_watersheds = cursor.fetchall()

    text = ""

    for watershed in all_watersheds:
        text += "-"*50
        text += "\n"
        text += f"🌊 {watershed[1]} 🌊\n"
        text += "-"*50
        text += "\n"

        cursor.execute("SELECT * FROM station WHERE watershed_id = ?", (watershed[0],))

        stations_from_hydro = cursor.fetchall()

        for station in stations_from_hydro:
            text += f'''
📍({station[2]}) - **{station[3]}**
├ Cota Atual: {station[4]} m
├ Cota Anterior: {station[6]} m
├ Variação Diária: ▼ -4 cm
├ Máximo Histórico: {station[8]} m ({station[9]})
├ Mínimo Histórico: {station[10]} m ({station[11]})
├ Diferença para Extremo (Mínima): 15.26 m
└ Última Atualização: {station[5]}\n'''

    print(text)
    conn.close()

def main():
    init_db()

    if not is_database_populated():
        populate_database()

    print("-" * 50)
    print("Atualizando informações das estações")
    print(f"Para o dia {parser_today_7h_utc}")
    print("-" * 50)

    for station, url in stations.items():
        update_all_station(station, url)

    generate_report()

if __name__ == "__main__":
    main()

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from datetime import datetime
import requests
import sqlite3


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_path = 'exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
final_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'



def log_progress(message):
    """Registra el progreso del código en el archivo de log"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    """Extrae la tabla de bancos por capitalización de mercado"""
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')  # Primera tabla = "By market capitalization"
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None:
                # Obtener nombre del banco (único enlace <a>)
                name = col[1].find('a').text
                # Obtener market cap, eliminar \n y convertir a float
                mc_value = col[2].text.strip()
                mc_float = float(mc_value)
                
                data_dict = {
                    "Name": name,
                    "MC_USD_Billion": mc_float
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    
    return df



def transform(df, exchange_rate_path):
    """Transforma los datos agregando columnas en GBP, EUR e INR"""
    # Paso 1: Leer CSV y convertir a diccionario
    exchange_rate_df = pd.read_csv(exchange_rate_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    # Paso 2: Agregar columna MC_GBP_Billion
    gbp_rate = float(exchange_rate['GBP'])
    df['MC_GBP_Billion'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']]
    
    # Paso 3: Agregar columna MC_EUR_Billion
    eur_rate = float(exchange_rate['EUR'])
    df['MC_EUR_Billion'] = [np.round(x * eur_rate, 2) for x in df['MC_USD_Billion']]
    
    # Paso 4: Agregar columna MC_INR_Billion
    inr_rate = float(exchange_rate['INR'])
    df['MC_INR_Billion'] = [np.round(x * inr_rate, 2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, csv_path):
    """Guarda el DataFrame en un archivo CSV"""
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    """Guarda el DataFrame en una tabla de base de datos SQLite"""
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query_statement, sql_connection):
    """Ejecuta una consulta SQL y muestra el resultado"""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    print()  # Línea en blanco para separar consultas



# ============ EJECUCIÓN PRINCIPAL ============

log_progress('Preliminaries complete. Initiating ETL process')

# Extraer datos
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

# Transformar datos
df = transform(df, exchange_rate_path)
log_progress('Data transformation complete. Initiating loading process')

# Cargar a CSV
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

# Conectar a base de datos SQLite
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

# Cargar a base de datos
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table')

# Ejecutar consultas
print("\n========== CONSULTAS SQL ==========\n")

# Consulta 1: Contenido de toda la tabla
query_statement = "SELECT * FROM Largest_banks"
run_queries(query_statement, sql_connection)

# Consulta 2: Promedio de MC en GBP
query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query_statement, sql_connection)

# Consulta 3: Nombres de los 5 principales bancos
query_statement = "SELECT Name FROM Largest_banks LIMIT 5"
run_queries(query_statement, sql_connection)

log_progress('Process Complete')

# Cerrar conexión
sql_connection.close()






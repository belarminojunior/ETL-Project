'''
Acquiring and Processing Information on the World's Largest Banks

In this project, we will work with real-world data and perform the operations
of Extraction, Transformation, and Loading (ETL) as required.

- Task 1: Logging function
- Task 2: Extraction of data
- Task 3: Transformation of data
- Task 4: Loading to CSV
- Task 5: Loading to Database
- Task 6: Function to Run queries on Database
- Task 7: Verify log entries
'''

import requests
import sqlite3
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

DATA_URL = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

TABLE_ATTRIBUTES = ['Name', 'MC_USD_Billion']
DB_NAME = 'src\project\Banks.db'
TABLE_NAME = 'Largest_banks'

CONN = sqlite3.connect(DB_NAME)

QUERY_STATEMENTS = [
    'SELECT * FROM Largest_banks',
    'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
    'SELECT Name from Largest_banks LIMIT 5'
]


LOG_FILE = 'src\project\log_file.txt'
EXCHANGE_RATE_PATH = 'src\project\exchange_rate.csv'
OUTPUT_CSV_PATH = 'src\project\Largest_banks_data.csv'


def log_progress(message):
    timeformat = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timeformat)

    with open(LOG_FILE, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attributes):
    df = pd.DataFrame(columns=table_attributes)

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    tables = data.find_all('tbody')[0]
    rows = tables.find_all('tr')

    for row in rows:
        col = row.find_all('td')

        if len(col) != 0:
            ancher_data = col[1].find_all('a')[1]
            if ancher_data is not None:
                data_dict = {
                    'Name': ancher_data.contents[0],
                    'MC_USD_Billion': col[2].contents[0]
                }

                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    USD_list = list(df['MC_USD_Billion'])
    USD_list = [float(''.join(x.split('\n'))) for x in USD_list]
    df['MC_USD_Billion'] = USD_list

    return df


def transform(df, exchange_rate_path):
    csvfile = pd.read_csv(exchange_rate_path)

    # i made here the content for currenct is the keys and the content of
    # the rate is the values to the crossponding keys
    dict = csvfile.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'], 2)
                            for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * dict['INR'], 2)
                            for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'], 2)
                            for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)


def run_query(query_statements, conn):
    for query in query_statements:
        print(query)
        print(pd.read_sql(query, conn), '\n')


def main():
    log_progress('Preliminaries complete. Initiating ETL process.')

    df = extract(DATA_URL, TABLE_ATTRIBUTES)
    log_progress(
        'Data extraction complete. Initiating Transformation process.')

    df = transform(df, EXCHANGE_RATE_PATH)
    log_progress('Data transformation complete. Initiating loading process.')

    load_to_csv(df, OUTPUT_CSV_PATH)
    log_progress('Data saved to CSV file.')

    log_progress('SQL Connection initiated.')

    load_to_db(df, CONN, TABLE_NAME)
    log_progress('Data loaded to Database as table. Running the query.')

    run_query(QUERY_STATEMENTS, CONN)
    CONN.close()
    log_progress('Process Complete.')


if __name__ == '__main__':
    main()

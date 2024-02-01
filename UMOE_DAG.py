'''
This program is made of automation of ETL from Postgre to Elasticsearch.
Data used is of superstore data in North America.
'''
import psycopg2
import re
import pandas as pd
import datetime as dt
import warnings
from elasticsearch import Elasticsearch
warnings.filterwarnings('ignore')

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#=================================================
# 1. Get data from postgresql
#=================================================
def get_data():
    '''
    This programs fetch table_m3 from the connected PostgreSQL server.

    Returns csv of said table in dags folder.

    Example of usage: get_data()
    '''
    # Config Database
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'

    # Connect to databse
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )

    # Get all data
    select_query = 'SELECT * FROM table_UMOE'
    df = pd.read_sql(select_query, connection)

    # Close the connection
    connection.close()

    # Save into CSV
    df.to_csv('opt/airflow/dags/data-raw.csv')

#=============================================
# 2. Clean data
#=============================================

def clean_data():
    '''
    This program do data cleaning on the fetched CSV with Pandas. Data cleaning done are:
     - convert columns into lowercase separated by underscore per word
     - drop duplicates
     - drop null

    Returns csv that has been cleaned/clean csv in dags folder.

    example of use: clean_data()
    '''
    # Make column name small letters separated by underscore per word
    df = pd.read_csv('opt/airflow/dags/data-raw.csv')
    cols = df.columns.tolist()
    new_cols = []

    for col in cols:
        new_col = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', col)
        new_col = [x.lower() for x in new_col]
        new_col = '_'.join(new_col)
        new_cols.append(new_col)
    df.columns = new_cols

    

    # drop duplicates
    df.drop_duplicates(inplace=True)

    # drop missing data
    df.dropna(inplace=True)

    #save cleaned data into csv
    df.to_csv('/opt/airflow/dags/data-clean.csv',index=False)

#===========================================
# 3. Insert data into elastic search
#===========================================

def insert_data_into_elastic():
    '''
    This program inserts cleaned data into elastic search.
    It first turn to CSV to pandas dataframe then to json then inputted to elasticsearch.

    Returns inputted data in elasticsearch.

    Example of use: insert_data_into_elastic()
    '''
    # read cleaned csv to pd dataframe
    df = pd.read_csv('/opt/airflow/dags/data-clean.csv')

    # check connection
    es = Elasticsearch("http://elasticsearch:9200")
    print('Connection status : ', es.ping())

    # insert CSV File to Elastic Search
    failed_insert = []
    for i, r in df.iterrows():
        doc = r.to_json()
        try:
            print(i)
            res = es.index(index="superstore_data", doc_type = 'doc', body=doc)
        except:
            print(' Failed Index : ', i)
            failed_insert.append(i)
            pass

    print('Done!')
    print('Failed Insert :', failed_insert)

#=============================================
# 4.Data Pipeline
#=============================================
default_args = {
    'owner': 'ade',
    'start_date': dt.datetime(2024, 1, 29, 19, 40, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG(
    'Phase 2 Milestone 3 Ade W T FTDS-RMT-026',
    default_args = default_args,
    schedule_interval = '30 6 * * *',
    catchup = False) as dag:
    '''
    This program knits together all the previous processes into one single DAG

    '''
    node_start = BashOperator(
        task_id = "starting",
        bash_command='echo: "reading the csv...')

    node_fetch_data = PythonOperator(
        task_id = 'get_data',
        python_callable=get_data)

    node_data_cleaning = PythonOperator(
        task_id = 'clean_data',
        python_callable=clean_data)

    node_insert_data_to_elastic = PythonOperator(
        task_id= 'insert_data_into_elastic',
        python_callable = insert_data_into_elastic)

node_start >> node_fetch_data >> node_data_cleaning >> node_insert_data_to_elastic





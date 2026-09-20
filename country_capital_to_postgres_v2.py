from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import timedelta
from datetime import datetime
import requests


def return_postgres_conn():

    # Initialize the PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    conn.autocommit = True

    return conn.cursor()


@task
def extract(url):
    f = requests.get(url)
    return (f.text)


@task
def transform(text):
    lines = text.strip().split("\n")
    records = []
    for l in lines:  # remove the first row
        (country, capital) = l.split(",")
        records.append([country, capital])
    return records[1:]


@task
def load(records, target_table):
    cur = return_postgres_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (country varchar primary key, capital varchar);")
        cur.execute(f"DELETE FROM {target_table}")
        for r in records:
            country = r[0].replace("'", "''")
            capital = r[1].replace("'", "''")
            print(country, "-", capital)

            sql = f"INSERT INTO {target_table} (country, capital) VALUES ('{country}', '{capital}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'CountryCaptial_Postgres_v2',
    start_date = datetime(2026,9,16),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "raw.country_capital"
    url = Variable.get("country_capital_url")
    
    data = extract(url)
    lines = transform(data)
    load(lines, target_table)

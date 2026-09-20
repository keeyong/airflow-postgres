from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import psycopg
import requests


def return_postgres_conn():

    user_id = Variable.get('postgres_userid')
    password = Variable.get('postgres_password')
    db = Variable.get('postgres_database')
    host = Variable.get('postgres_host')

    # Establish a connection to Postgres
    conn = psycopg.connect(
        host=host,
        port=5432,
        dbname=db,
        user=user_id,
        password=password,
        autocommit=True,
        sslmode="require",
    )
    # Create a cursor object
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
    cur = return_postgres_cursor()
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
    dag_id = 'CountryCaptial_postgres',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ETL'],
    schedule = '0 2 * * *'
) as dag:
    target_table = "raw.country_capital"
    url = Variable.get("country_capital_url")

    data = extract(url)
    lines = transform(data)
    load(lines, target_table)

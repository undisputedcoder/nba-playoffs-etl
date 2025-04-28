from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2

url = "https://www.basketball-reference.com/playoffs/NBA_2024_per_game.html"

def dummy_callable(params):
    return f"{params["action"]} pipeline"

def extract_transform_data():
    playoff_stats = pd.read_html(url, header=0, attrs={'id': 'per_game_stats'})[0]
    playoff_stats = playoff_stats[(playoff_stats['Rk'] != '') & (playoff_stats['Rk'].str.isnumeric())]
    playoff_stats = playoff_stats.drop(columns="Rk")
    playoff_stats = playoff_stats.replace(np.nan, 0.0)
    playoff_stats.to_csv("2024_playoff_stats.csv", index=False)
    
def load_data_in_db():
    db_config = {
        "dbname": "2024_playoffs",
        "user": "postgres",
        "password": "postgres",
        "host": "odb", 
        "port": 5432,
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        curr = conn.cursor()
        curr.execute(open("/opt/airflow/dags/sql/create_tables.sql", "r").read())
        
        with open("/opt/airflow/2024_playoff_stats.csv", "r") as f:
            next(f)
            curr.copy_from(f, 'playoffs_stats_2024', sep=',')
        conn.commit()
        
    except Exception as e:
        print("Error:", e)
    
with DAG(
        "2024_nba_playoffs",
        description="Extracting stats about the 2024 NBA playoffs from basketball-reference",
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:

    start = PythonOperator(
        task_id="starting_pipeline",
        python_callable=dummy_callable,
        params={"action": "starting"},
        dag=dag
    )

    extract_transform_data = PythonOperator(
        task_id="extract_transform_data",
        python_callable=extract_transform_data,
        dag=dag
    )
    
    load_data_in_db = PythonOperator(
        task_id="load_data_in_db",
        python_callable=load_data_in_db,
        dag=dag
    )
    
    end = PythonOperator(
        task_id="ending_pipeline",
        python_callable=dummy_callable,
        params={"action": "ending"},
        dag=dag
    )

    start >> extract_transform_data >> load_data_in_db >> end
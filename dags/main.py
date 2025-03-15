import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from sheet_spool import upload_to_googlesheet
import glob
import pandas as pd
import io
import csv

default_args = {
    "owner": "opejoseph",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 12),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "migrate_transactions",
    default_args=default_args,
    description="A DAG to generate and save dummy transaction data",
    schedule_interval="0 12 * * *",
    catchup=False,
)

script_path = "/opt/airflow/dags/generate_transactions.py" 

def upload_to_postgres(**context):

    POSTGRES_CONN_ID = "postgres_default"
    TABLE_NAME = "supermarket_transactions"
    

    transactions_dir = "/opt/airflow/dummy_transactions"
    list_of_files = glob.glob(os.path.join(transactions_dir, "supermarket_transactions_*.csv"))
    
    if not list_of_files:
        raise FileNotFoundError(f"No transaction files found in {transactions_dir}")
    

    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Loading file: {latest_file}")
    
  
    df = pd.read_csv(latest_file)
    

    postgres_df = df[df["Platform"] == "PostgreSQL"].copy()
    
    if postgres_df.empty:
        print("No PostgreSQL platform data found in this file. Nothing to upload.")
        return
    
    print(f"Found {len(postgres_df)} rows with PostgreSQL platform out of {len(df)} total rows")
    

    pg_hook = PostgresHook(postgres_conn_id="aiven_postgres")
    
    try:

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            "Invoice Number" INTEGER,
            "Invoice Date" DATE,
            "Customer Name" VARCHAR(100),
            "Product Name" VARCHAR(100),
            "Quantity" INTEGER,
            "Unit Price" DECIMAL(10,2),
            "Total Amount" DECIMAL(10,2),
            "Platform" VARCHAR(50),
            "Last Update" TIMESTAMP,
            "Upload Date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """.format(TABLE_NAME)
        
        pg_hook.run(create_table_sql)
        

        postgres_df["Upload Date"] = datetime.now()
        
  
        postgres_df["Invoice Date"] = pd.to_datetime(postgres_df["Invoice Date"])
        

        buffer = io.StringIO()
        postgres_df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
        buffer.seek(0)
        

        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        

        columns = ', '.join([f'"{col}"' for col in postgres_df.columns])
        

        copy_sql = f"COPY {TABLE_NAME} ({columns}) FROM STDIN WITH CSV"
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
        
        print(f"Successfully uploaded {len(postgres_df)} rows to {TABLE_NAME}")
        
 
        with open(os.path.join(transactions_dir, "upload_log.txt"), "a") as log:
            log.write(f"{datetime.now()}: Uploaded {len(postgres_df)} PostgreSQL platform rows from {latest_file} to PostgreSQL database\n")
            
    except Exception as e:
        print(f"Error uploading data to PostgreSQL: {e}")
        raise
    
    return True


def extract_sheet_data(**kwargs):
    spreadsheet_id = "1RoB52Rk-71uQiuplol7yhLvgWHFv3v079u-yMEQEu8o"
    rows = upload_to_googlesheet(spreadsheet_id)
    print(f"Extracted {len(rows)} rows from Google Sheet")
    return rows

def process_sheet_data(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='extract_sheet_data')
    df = pd.DataFrame(rows)
    print(f"Processing {len(df)} rows")
    results = df.to_dict('records')
    return results


run_script = BashOperator(
    task_id="run_generate_transactions",
    bash_command=f"python {script_path}",
    dag=dag,
)


upload_data = PythonOperator(
    task_id="upload_to_postgres",
    python_callable=upload_to_postgres,
    provide_context=True,
    dag=dag,
)

extract_googlesheet = PythonOperator(
    task_id="Extract Googlesheet",
    python_callable=extract_sheet_data,
    provide_context=True,
    dag=dag
)

process_sheet_data = PythonOperator(
    task_id="Progress Googlesheet",
    python_callable=process_sheet_data,
    provide_context=True,
    dag=dag
)



run_script >> upload_data
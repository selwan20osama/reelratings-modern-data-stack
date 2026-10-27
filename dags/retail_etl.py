import os
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# استخدام مسار ثابت يضمن توفر المجلد داخل Docker
DATA_DIR = '/opt/airflow/dags/data'
os.makedirs(DATA_DIR, exist_ok=True)

ORDERS_FILE = os.path.join(DATA_DIR, 'orders.csv')
PRODUCTS_FILE = os.path.join(DATA_DIR, 'products.json')
DB_FILE = os.path.join(DATA_DIR, 'retail_dw.db')
REJECTED_FILE = os.path.join(DATA_DIR, 'rejected_orders.csv')
REPORT_FILE = os.path.join(DATA_DIR, 'daily_sales_report.csv')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

def check_sources():
    # إنشاء ملفات بيانات افتراضية فوراً في حال عدم وجودها لمنع استثناء FileNotFoundError
    if not os.path.exists(ORDERS_FILE):
        df_orders = pd.DataFrame([
            {'order_id': 1, 'product_id': 101, 'quantity': 2, 'order_date': '2026-10-10', 'customer_id': 1},
            {'order_id': 2, 'product_id': 102, 'quantity': 5, 'order_date': '2026-10-10', 'customer_id': 2},
            {'order_id': 3, 'product_id': 101, 'quantity': 1, 'order_date': '2026-10-10', 'customer_id': 3}
        ])
        df_orders.to_csv(ORDERS_FILE, index=False)
        
    if not os.path.exists(PRODUCTS_FILE):
        products_data = [
            {'product_id': 101, 'product_name': 'Laptop', 'price': 1000},
            {'product_id': 102, 'product_name': 'Mouse', 'price': 50}
        ]
        with open(PRODUCTS_FILE, 'w') as f:
            json.dump(products_data, f)

    print("All source files exist and verified.")

def extract_orders():
    df = pd.read_csv(ORDERS_FILE)
    print(f"Extracted {len(df)} orders.")

def extract_products():
    with open(PRODUCTS_FILE, 'r') as f:
        data = json.load(f)
    print(f"Extracted {len(data)} products.")

def clean_and_join():
    orders = pd.read_csv(ORDERS_FILE)
    with open(PRODUCTS_FILE, 'r') as f:
        products = pd.DataFrame(json.load(f))
    
    min_qty = int(Variable.get("min_quantity", default_var=1))
    
    valid_orders = orders[orders['quantity'] >= min_qty]
    rejected_orders = orders[orders['quantity'] < min_qty]
    
    rejected_orders.to_csv(REJECTED_FILE, index=False)
    
    merged = pd.merge(valid_orders, products, on='product_id', how='inner')
    merged['total_revenue'] = merged['quantity'] * merged['price']
    
    conn = sqlite3.connect(DB_FILE)
    merged.to_sql('fact_sales', conn, if_exists='replace', index=False)
    conn.close()
    
    report = merged.groupby('order_date').agg(
        total_revenue=('total_revenue', 'sum'),
        total_orders=('order_id', 'count')
    ).reset_index()
    report.to_csv(REPORT_FILE, index=False)
    print("Clean and Join completed successfully.")

def top_customers():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT customer_id, SUM(total_revenue) as revenue FROM fact_sales GROUP BY customer_id ORDER BY revenue DESC LIMIT 3", conn)
    conn.close()
    print("--- TOP 3 CUSTOMERS ---")
    print(df.to_string(index=False))

with DAG(
    'retail_etl',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    task_check = PythonOperator(
        task_id='check_sources',
        python_callable=check_sources
    )

    with TaskGroup('extract') as extract_group:
        task_extract_orders = PythonOperator(
            task_id='extract_orders',
            python_callable=extract_orders
        )
        task_extract_products = PythonOperator(
            task_id='extract_products',
            python_callable=extract_products
        )

    task_clean_join = PythonOperator(
        task_id='clean_and_join',
        python_callable=clean_and_join
    )

    task_top_cust = PythonOperator(
        task_id='top_customers',
        python_callable=top_customers
    )

    task_check >> extract_group >> task_clean_join >> task_top_cust
from airflow import DAG
from airflow.utils.dates import days_ago
from operators.raizen_operator import RaizenOperator
from datetime import timedelta
from scripts.create_bucket import create_bucket
from scripts.load_landing_zone import load_landing_zone
from scripts.bronze_etl_diesel import bronze_etl_diesel
from scripts.bronze_etl_oil_dev_fuels import bronze_etl_oil_dev_fuels
from scripts.silver_etl_partition import silver_etl_partition

default_args = {
    'owner': 'Douglas Amorim',
    'email': 'douglas_amorimm@outlook.com',
    'depends_on_past': True,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5)
}

with DAG(
        "pipeline_raizen",
        default_args=default_args,
        start_date=days_ago(2),
        schedule_interval='@daily',
        catchup=True
    ) as dag:
    
    task_create_bucket = RaizenOperator(
            task_id='create_bucket',
            python_callable=create_bucket,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            dag=dag,
        )
    
    task_landing_zone_oil_dev_fuels = RaizenOperator(
            task_id='landing_zone_oil_dev_fuels',
            python_callable=load_landing_zone,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            op_kwargs={
                    'sbn' : 'landing-zone', 
                    'son' : 'oil-derivative-fuels', 
                    'src' : 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-2000-2023.csv'
                },
            provide_context=True,
            dag=dag,
        )

    task_landing_zone_diesel = RaizenOperator(
            task_id='landing_zone_diesel',
            python_callable=load_landing_zone,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            op_kwargs={
                    'sbn' : 'landing-zone', 
                    'son' : 'diesel', 
                    'src' : 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-oleo-diesel-tipo-m3-2020.csv'
                },
            provide_context=True,
            dag=dag,
        )
    
    task_bronze_etl_oil_dev_fuels = RaizenOperator(
            task_id='bronze_etl_oil_dev_fuels',
            python_callable=bronze_etl_oil_dev_fuels,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            op_kwargs={
                    'sbn' : 'landing-zone', 
                    'dbn' : 'bronze', 
                    'obj' : 'oil-derivative-fuels.csv'
                },
            provide_context=True,
            dag=dag,
        )

    task_bronze_etl_diesel = RaizenOperator(
            task_id='bronze_etl_diesel',
            python_callable=bronze_etl_diesel,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            op_kwargs={
                    'sbn' : 'landing-zone', 
                    'dbn' : 'bronze', 
                    'obj' : 'diesel.csv'
                },
            provide_context=True,
            dag=dag,
        )
    
    task_silver_etl_oil_dev_fuels = RaizenOperator(
            task_id='silver_etl_oil_dev_fuels',
            python_callable=silver_etl_partition,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            op_kwargs={
                    'sbn' : 'bronze', 
                    'dbn' : 'silver',
                    'part': 'oil-derivative-fuels',
                    'obj' : 'oil-derivative-fuels.csv'
                },
            provide_context=True,
            dag=dag,
        )

    task_silver_etl_diesel = RaizenOperator(
            task_id='silver_etl_diesel',
            python_callable=silver_etl_partition,
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            op_kwargs={
                    'sbn' : 'bronze', 
                    'dbn' : 'silver',
                    'part': 'diesel',
                    'obj' : 'diesel.csv'
                },
            provide_context=True,
            dag=dag,
        )
    
    task_create_bucket >> [task_landing_zone_oil_dev_fuels, task_landing_zone_diesel]
    task_landing_zone_oil_dev_fuels >> task_bronze_etl_oil_dev_fuels >> task_silver_etl_oil_dev_fuels
    task_landing_zone_diesel >> task_bronze_etl_diesel >> task_silver_etl_diesel
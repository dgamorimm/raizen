import sys
import os

diretorio_raiz = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(diretorio_raiz))

from airflow import DAG
from airflow.utils.dates import days_ago
from operators.raizen_operator import RaizenOperator
from datetime import timedelta

default_args = {
    'owner': 'Douglas Amorim',
    'email': 'douglas_amorimm@outlook.com',
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        "pipeline_raizen",
        default_args=default_args,
        start_date=days_ago(2),
        schedule_interval='@daily',
        catchup=True
    ) as dag:
    
    create_bucket = RaizenOperator(
            task_id='create_bucket',
            python_script='../scripts/create_bucket.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            dag=dag,
        )
    
    landing_zone_oil_dev_fuels = RaizenOperator(
            task_id='landing_zone_oil_dev_fuels',
            python_script='../scripts/load-landing-zone.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            sbn_parameter='landing-zone',
            son_parameter='oil-derivative-fuels',
            src_parameter='https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-2000-2023.csv',
            dag=dag,
        )

    landing_zone_diesel = RaizenOperator(
            task_id='landing_zone_diesel',
            python_script='../scripts/load-landing-zone.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            sbn_parameter='landing-zone',
            son_parameter='diesel',
            src_parameter='oil-derivative-fuels.csv',
            dag=dag,
        )
    
    bronze_etl_oil_dev_fuels = RaizenOperator(
            task_id='bronze_etl_oil_dev_fuels',
            python_script='../scripts/bronze-etl-oil-dev-fuels.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            sbn_parameter='landing-zone',
            dbn_parameter='bronze',
            obj_parameter='diesel.csv',
            dag=dag,
        )

    bronze_etl_diesel = RaizenOperator(
            task_id='bronze_etl_diesel',
            python_script='../scripts/bronze-etl-diesel.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            sbn_parameter='landing-zone',
            dbn_parameter='bronze',
            obj_parameter='diesel.csv',
            dag=dag,
        )
    
    silver_etl_oil_dev_fuels = RaizenOperator(
            task_id='silver_etl_oil_dev_fuels',
            python_script='../scripts/silver-etl-partition.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            sbn_parameter='bronze',
            dbn_parameter='silver',
            part_parameter='oil-derivative-fuels',
            obj_parameter='oil-derivative-fuels.csv',
            dag=dag,
        )

    silver_etl_diesel = RaizenOperator(
            task_id='silver_etl_diesel',
            python_script='../scripts/silver-etl-partition.py',
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
            sbn_parameter='bronze',
            dbn_parameter='silver',
            part_parameter='diesel',
            obj_parameter='diesel.csv',
            dag=dag,
        )
    
    create_bucket >> [landing_zone_oil_dev_fuels, landing_zone_diesel]
    landing_zone_oil_dev_fuels >> bronze_etl_oil_dev_fuels >> silver_etl_oil_dev_fuels
    landing_zone_diesel >> bronze_etl_diesel >> silver_etl_diesel
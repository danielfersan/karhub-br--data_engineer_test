from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator)
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow import DAG
import os

project_id = 'thematic-bloom-407120'

with DAG(
    'minha_dag',
    default_args={
        'owner': 'Daniel Fernandes',
        'depends_on_past': False,
        'start_date': datetime(2024, 9, 10)
    },
    schedule_interval=timedelta(days=1),
) as dag:

    # Tarefa para executar o código Python que coleta as informações da API
    run_get_data_api = BashOperator(
        task_id='run_get_data_api',
        bash_command='python /opt/airflow/scripts/get_data_api.py'
    )

    # Tarefa para criar o dataset no bigquery
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset",
        gcp_conn_id="google_cloud_default",
        dataset_id="karhub",
        location="US",
    )


    # Tarefa para executar o código Python que trata os dados, converte os valores e grava as informações no Bigquery
    run_raw_to_trusted = BashOperator(
        task_id='run_raw_to_trusted',
        bash_command='python /opt/airflow/scripts/raw_to_trusted.py'
    )

    # Tarefas para criação de tabelas no bigquery
    create_table_top_arrecadacao = BigQueryInsertJobOperator(
        task_id="query_top_arrecadacao",
        configuration={
            "query": {
                "query": f'''
            CREATE OR REPLACE TABLE `{project_id}.karhub.top_arecadacao` AS
            SELECT id_fonte_recurso, nome_fonte_recurso, total_arrecadado
            FROM `{project_id}.karhub.fonte_recursos_detalhamento`
            order by total_arrecadado desc
            limit 5        
            ''',
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id="google_cloud_default",
        location='US'
    )

    create_table_top_gastos = BigQueryInsertJobOperator(
    task_id="query_top_gastos",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.top_gastos` AS
        SELECT id_fonte_recurso, nome_fonte_recurso, total_liquidado
        FROM `{project_id}.karhub.fonte_recursos_detalhamento`
        order by total_liquidado desc
        limit 5        
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    create_table_top_magem_bruta = BigQueryInsertJobOperator(
    task_id="query_top_margem_bruta",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.top_magem_bruta` AS
        select * from (
            SELECT 
            id_fonte_recurso, nome_fonte_recurso,
            (total_arrecadado/nullif(total_liquidado,0)) * 100 as margem_bruta
             FROM `{project_id}.karhub.fonte_recursos_detalhamento` 
            )
            order by margem_bruta desc
            limit 5
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    create_table_top_menor_arrecadacao = BigQueryInsertJobOperator(
    task_id="query_top_menor_arrecadacao",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.top_menor_arrecadacao` AS
            SELECT 
            id_fonte_recurso, nome_fonte_recurso,
            total_arrecadado
             FROM `{project_id}.karhub.fonte_recursos_detalhamento` 
            order by total_arrecadado asc
            limit 5
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    create_table_top_menor_gastos = BigQueryInsertJobOperator(
    task_id="query_top_menor_gastos",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.top_menor_gastos` AS
            SELECT 
            id_fonte_recurso, nome_fonte_recurso,
            total_liquidado
             FROM `{project_id}.karhub.fonte_recursos_detalhamento` 
            order by total_liquidado asc
            limit 5
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    create_table_top_pior_magem_bruta = BigQueryInsertJobOperator(
    task_id="query_top_pior_margem_bruta",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.top_pior_magem_bruta` AS
        select * from (
            SELECT 
            id_fonte_recurso, nome_fonte_recurso,
            (total_arrecadado/nullif(total_liquidado,0)) * 100 as margem_bruta
             FROM `{project_id}.karhub.fonte_recursos_detalhamento` 
            )
            order by margem_bruta asc
            limit 5
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    create_table_media_de_arrecadacao = BigQueryInsertJobOperator(
    task_id="query_media_de_arrecadacao",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.media_de_arrecadacao` AS
        select * from (
            select 
            left(fonte_de_recursos,3) as id_fonte_recursos,
            substr(fonte_de_recursos,6,length(fonte_de_recursos)) as nome_fonte_de_recursos,
            media_arrecadado
            from(
            SELECT `Fonte de Recursos` as fonte_de_recursos,
            AVG(total_arrecadado) media_arrecadado FROM `{project_id}.karhub.gdvReceitas` 
            group by 1))
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    create_table_media_de_despesas = BigQueryInsertJobOperator(
    task_id="query_media_de_despesas",
    configuration={
        "query": {
            "query": f'''
        CREATE OR REPLACE TABLE `{project_id}.karhub.media_de_despesas` AS
        select * from (
            select 
            left(fonte_de_recursos,3) as id_fonte_recursos,
            substr(fonte_de_recursos,6,length(fonte_de_recursos)) as nome_fonte_de_recursos,
            media_liquidado
            from(
            SELECT `Fonte de Recursos` as fonte_de_recursos,
            AVG(total_liquidado) media_liquidado FROM `{project_id}.karhub.gdvDespesas` 
            group by 1))
        ''',
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id="google_cloud_default",
    location='US'
    )

    # Definindo as dependências entre as tarefas
    run_get_data_api >> create_dataset >> run_raw_to_trusted >> create_table_top_arrecadacao >> create_table_top_gastos >> create_table_top_magem_bruta >> create_table_top_menor_arrecadacao >> create_table_top_menor_gastos >> create_table_top_pior_magem_bruta >> create_table_media_de_arrecadacao >> create_table_media_de_despesas

from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

# Pega os parametros do arquivo .env
path_despesas = os.getenv('path_file_despesas')
path_receitas = os.getenv('path_file_receitas')
table_id = os.getenv('table_id')
project_id = os.getenv('project_id')
path_file_sa = os.getenv('path_file_sa')
path_file_cotacao_dolar = os.getenv('path_file_cotacao_dolar')

# Utiliza a conta de serviço para se conectar ao GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_file_sa
client = bigquery.Client()

# Coleta o valor do dolar na tabela de valores de dolar
df_valor_dolar = pd.read_csv(path_file_cotacao_dolar,sep= ';')
df_valor_dolar = df_valor_dolar[df_valor_dolar['date'] == '2022-06-22']
valor_dolar = df_valor_dolar['high'].tolist()[0]


# Tratametno da tabela de receitas - E grava no Bigquery a tabela gdvReceitas
df_receitas = pd.read_csv(path_receitas)
df_receitas = df_receitas.dropna().rename(columns={'Arrecadado': 'total_arrecadado'})
df_receitas['total_arrecadado'] = (df_receitas['total_arrecadado'].str.replace('.','').str.replace(',','.').astype(float))
df_receitas['total_arrecadado'] = df_receitas['total_arrecadado'] * valor_dolar
df_receitas.to_gbq(f'{table_id}.gdvReceitas',project_id=project_id, if_exists='replace')
df_receitas = df_receitas.groupby('Fonte de Recursos')['total_arrecadado'].sum()
df_receitas = df_receitas.reset_index()

# Tratamento da tabela de despesas - E grava no Bigquery a tabela gdvDespesas
df_despesas = pd.read_csv(path_despesas)
df_despesas = df_despesas[['Fonte de Recursos','Liquidado']]
df_despesas = df_despesas.dropna().rename(columns={'Liquidado': 'total_liquidado'})
df_despesas['total_liquidado'] = (df_despesas['total_liquidado'].str.replace('.','').str.replace(',','.').astype(float))
df_despesas['total_liquidado'] = df_despesas['total_liquidado'] * valor_dolar
df_despesas.to_gbq(f'{table_id}.gdvDespesas',project_id=project_id, if_exists='replace')
df_despesas = df_despesas.groupby('Fonte de Recursos')['total_liquidado'].sum()
df_despesas = df_despesas.reset_index()


# Junção da tabela de despesas com a de receitas
df_result = pd.merge(df_receitas,df_despesas, on='Fonte de Recursos')
df_result['id_fonte_recurso'] = df_result['Fonte de Recursos'].str.slice(stop=3)
df_result['nome_fonte_recurso'] = df_result['Fonte de Recursos'].str.slice(start=6)
df_result = df_result.drop(['Fonte de Recursos'], axis=1)

# Adiciona a coluna de timestamp como pedido no desafio
df_result['dt_insert'] = pd.Timestamp.now()

# Carrega o dataframe no bigquery
df_result.to_gbq(f'{table_id}.fonte_recursos_detalhamento',project_id=project_id, if_exists='replace')

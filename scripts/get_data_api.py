from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import requests
import os

load_dotenv()

moeda = os.getenv('moeda')
start_date = os.getenv('start_date')
end_date = os.getenv('end_date')
path_file_cotacao_dolar = os.getenv('path_file_cotacao_dolar')

# Chama os registros da API
def call_api(coin, start_date,end_date):
    url = f"https://economia.awesomeapi.com.br/json/daily/{coin}?start_date={start_date}&end_date={end_date}"

    response = requests.request("GET", url)

    return response.json()

# Abre e pega os dados da tabela que armazena os valores das moedas
def get_table(path):
    df = pd.read_csv(path,sep=';')
    return df

# Transforma a resposta da API em um dataframe pandas
def create_tabe(data):
    df = pd.DataFrame(data)
    return df

# Salva o dataframe pandas em uma arquivo csv
def save_table(df):
    df.to_csv(path_file_cotacao_dolar,sep=';', index=False)

data = call_api(moeda,start_date,end_date)

print(data)

new_df = create_tabe(data)

# Converte o timestamp que veio em valor numerico para texto
new_df['timestamp'] = pd.to_numeric(new_df['timestamp'])
new_df['timestamp'] = pd.to_datetime(new_df['timestamp'],unit='s')
new_df['date'] = new_df['timestamp'].dt.date

# Verifica se o csv com os registros da moeda já existem, caso exista ele vai coletar os dados e dar join com os novos registros, caso não exista ele vai criar uma arquivo novo.
if os.path.exists(path_file_cotacao_dolar):
    old_df = get_table(path_file_cotacao_dolar)
    join_df = pd.concat([old_df,new_df], ignore_index=True)

else:
    join_df = new_df

export_df = join_df.drop_duplicates(subset='timestamp',keep=False)

save_table(export_df)

# karhub-br-data_engineer_test
## Introdução
Teste realizado por Daniel Fernandes dos Santos

Para rodar o projeto, clone esse git para uma pasta, acesse a pasta com esses arquivos pelo terminal e execute os seguintes comandos:

```
docker compose up airflow-init
docker compose up 
 ```

Acerca da explicação do código, se basear nos comentarios do mesmo.

## Arquitetura
Eu montei o projeto considerando o seguinte ETL:
- Utilizar o imagem doker de airflow para orquestrar todo o processo;
- Um código python de nome get_data_api, para coletar os dados da API e gravar em um arquivo csv;
- Um código python de nome raw_to_trusted, que abre os 2 arquivos csv (gdvDespesas e gdvReceitas), os higieniza e os grava no bigquery utilizando pandas.
- Um código de nome Orchestrator, que é uma DAG a qual orquestra todo o processo inicialmente chamando os códigos python, depois executando algumas querys dentro do bigquery.

## Considerações importantes
- Como as respostas das perguntas são tabelas no bigquery, eu criei um looker studio para poderem visualizar as respostas com mais facilidade.
- Caso queiram rodar o código em um projeto de seu interesse é preciso realizar as seguintes alterações:
  - Acessar o .env e alterar o nome do projeto, e o path da conta de serviço;
  - Acessar a dag de nome orchestrator e alterar a variável de nome do projeto;
  - Adicione a conta de serviço que está faltando na pasta.

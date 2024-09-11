# karhub-br-data_engineer_test
## Introdução
Teste realizado por Daniel Fernandes dos Santos

Para rodar o projeto, clone esse git para uma pasta, acesse a pasta com esses arquivos pelo terminal e execute os seguintes comandos:

```
docker compose up airflow-init
docker compose up 
 ```

Acerca da explicação do código, se basear nos comentarios do mesmo.

## Considerações importantes
- Como as repostas das perguntas são tabelas no bigquery, eu criei um looker studio para poderem visualizar as respostas com mais facilidades.
- Caso queiram rodar o código em um projeto de seu interesse é preciso realizar as seguintes alterações:
  - Acessar o .env e alterar o nome do projeto, e o path da conta de serviço;
  - Acessar a dag de nome orchestrator e alterar a variavel de nome do projeto;
  - alterar a conta de serviço que esta na pasta data.

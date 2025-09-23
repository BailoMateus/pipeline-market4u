# Pipeline de Dados com Airflow - Desafio Técnico

## Visão Geral
Este projeto implementa um pipeline de dados baseado em Apache Airflow e Postgres, organizado em camadas:
- Bronze: ingestão dos arquivos CSV originais (dados crus).
- Silver: limpeza, padronização e tipagem das tabelas.
- Gold: construção de um Data Mart analítico (dm_vendas_clientes) com métricas RFM e visão 360º dos clientes.

---

## Relação entre as tabelas
Abaixo está um diagrama que mostra a relação dos dados utilizados no pipeline:

![Relação entre as tabelas](img_relacao_db.png)

## Estrutura do Projeto
.
├── dags/
│ ├── ingestao_bronze.py
│ ├── processamento_silver.py
│ └── carga_gold.py
├── sql/
│ ├── silver/
│ │ ├── customers_to_silver.sql
│ │ ├── geolocation_to_silver.sql
│ │ ├── order_items_to_silver.sql
│ │ ├── order_payments_to_silver.sql
│ │ ├── order_reviews_to_silver.sql
│ │ ├── orders_to_silver.sql
│ │ ├── products_to_silver.sql
│ │ ├── sellers_to_silver.sql
│ │ └── product_category_name_translation_to_silver.sql
│ └── gold/
│ └── make_dm_vendas_clientes.sql
├── data/
│ ├── olist_orders_dataset.csv
│ └── olist_customers_dataset.csv
├── query_teste/
│ ├── agregacao.sql
│ ├── categoria_principal.sql
│ └── pedidos_de_clientes.sql
├── logs/
├── plugins/
├── docker-compose.yaml
├── requirements.txt
├── .env
├── .gitignore
├── img_relacao_db.png
├── README.md
└── ANALISE.md

---

## Como Executar o Projeto

1. Clonar o repositório:
   git clone https://github.com/<seu_usuario>/<seu_repo>.git
   cd <seu_repo>

2. Subir os containers:
   docker-compose up -d

3. Acessar o Airflow:
   - URL: http://localhost:8080
   - Usuário/Senha: airflow / airflow

4. Executar as DAGs:
   - Ativar e rodar ingestao_bronze
   - Ela dispara automaticamente processamento_silver
   - Que por sua vez dispara carga_gold
   - Resultado final: tabela gold.dm_vendas_clientes populada no Postgres

---

## Aviso sobre o .env
O arquivo `.env` contém apenas a variável `AIRFLOW_UID=1000`, usada para compatibilidade de permissões no Docker.  
Ele foi incluído no repositório para facilitar a execução do desafio técnico.  
Em cenários reais, outros segredos e credenciais **nunca** devem ser versionados.

## Arquitetura

- Bronze → Silver → Gold com DAGs independentes mas conectadas por TriggerDagRunOperator
- Decisões de Silver:
  - Normalização de customer_city (lower(trim(...)))
  - Padronização de customer_state em uppercase
  - Limpeza de zip_code_prefix e seller_zip_code_prefix via regexp_replace + ::TEXT
  - Conversão de colunas de datas para TIMESTAMP
- Gold:
  - Construção de métricas RFM no dm_vendas_clientes
  - Uso de CTEs e funções analíticas (ROW_NUMBER, AVG, MIN, MAX)

---

## Dependências

- Apache Airflow 2.x
- Postgres 13
- Python 3.8+

Instaladas automaticamente via docker-compose.

### Dependências adicionais
As bibliotecas Python específicas usadas no projeto estão listadas em `requirements.txt`.  
Elas podem ser instaladas manualmente dentro do container do Airflow, caso necessário:  

pip install -r requirements.txt

---

## Testes

- Testado rodando localmente com Docker.
- Todas as DAGs foram executadas de ponta a ponta, produzindo a tabela dm_vendas_clientes no schema gold.

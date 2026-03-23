# Pipeline de Engenharia de Dados com Databricks | Arquitetura Medalhão



## Visão Geral
Pipeline completo de dados com Databricks, incluindo ingestão, transformação, modelagem dimensional (SCD Tipo 2) e orquestração com DAG.

Os dados são ingeridos a partir de um banco PostgreSQL, tratados e transformados, culminando em um modelo dimensional com SCD Tipo 2 e tabela fato.

---

## Arquitetura

- **Bronze**: Ingestão de dados brutos do PostgreSQL  
- **Silver**: Limpeza, padronização e deduplicação  
- **Gold**:
  - Dimensões (com SCD Tipo 2)
  - Tabela fato com chaves substitutas (Surrogate Keys)

---

## Tecnologias Utilizadas

- Databricks (Delta Lake)
- PySpark / SQL
- PostgreSQL (fonte de dados)
- Delta Tables
- Databricks Workflows (Jobs)

---

## Fluxo do Pipeline

1. Ingestão de dados do PostgreSQL → Bronze  
2. Transformação e limpeza → Silver  
3. Construção das dimensões (SCD Tipo 2) → Gold  
4. Construção da tabela fato com chaves substitutas  
5. Orquestração com dependências via Databricks Jobs  

---

## Conceitos Aplicados

- Arquitetura Medalhão 
- Slowly Changing Dimension Tipo 2 (SCD2)  
- Chaves substitutas (Surrogate Keys)  
- Carga incremental com MERGE  
- Deduplicação com hash  
- Orquestração em DAG  

---

## Modelo de Dados

### Tabela Fato
- customer_sk  
- date_sk  
- product_sk  
- price  
- transaction_hash  

### Dimensões
- dim_customer (SCD Tipo 2)  
- dim_product  
- dim_date  

---

## Orquestração

O pipeline é orquestrado utilizando Databricks Jobs com dependências entre tarefas:

---

## Destaques

- Pipeline completo de ponta a ponta  
- Modelagem dimensional consistente  
- Uso de boas práticas de engenharia de dados  
- Estrutura escalável e organizada  

---

## Melhorias Futuras

- Uso de parâmetros seguros (secrets)  
- Validação de qualidade dos dados  
- Otimização com particionamento e ZORDER  
- Integração com ferramentas de BI  

---

## Autor

Manoel Alexandre Peres

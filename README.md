### desafio-spark

Este repositório contém a resolução do desafio de Data Engineer.

Linguagem: Python-3.6 \
Framework: Spark-3.0

Descrição sobre o processo criado:
- Foi desenvolvido um processo de ETL utilizando spark em modo standalone rodando em ambiente local. \
Foi escolhido ambiente local devido a baixa volumetria do desafio e o custo.

- Leitura de arquivo CSV
- Deduplicação de registros
- Conversão dos data types
- Gravação de arquivo em Parquet. Foi escolhido o formato Parquet devido a ser um formato colunar de que possui esquema interno, suporte a compressão de dados e de alta performance.
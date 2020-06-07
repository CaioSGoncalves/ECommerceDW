# ECommerceDW
Implementação de um DataWarehouse a partir do dataset disponibilizado pela Olist.

Dataset: https://www.kaggle.com/olistbr/brazilian-ecommerce

**Links utilizados:**
- https://sejaumdatascientist.com/como-fazer-modelagem-de-dados-para-data-engineering/
- https://stackoverflow.com/questions/2507289/time-and-date-dimension-in-data-warehouse
- https://towardsdatascience.com/building-a-modern-batch-data-warehouse-without-updates-7819bfa3c1ee


![olist_model](model/olist_model.png)


**Objetivo:**
- Ingestão dos dados .csv no banco MySQL (db: sourceDB).
- Ingestão dos dados do sourceDB num Data Warehouse (simulado dentro do MySQL no db: dw).
- Executar queries analíticas com o objetivo de verificar o desempenho.
- Aprender sobre modelagem dimensional e star-schema.

**Resultados:**&nbsp;
Query Número de Itens de Pedido Por Dia:
- Tempo de processamento no sourceDB -> 0.364 sec
- Tempo de processamento no dw -> 0.179 sec
&nbsp;
Query Número de Itens de Pedido por Localidade:
- Tempo de processamento no sourceDB -> 0.501 sec
- Tempo de processamento no dw -> 0.271 sec

**Diagrama sourceDB:**
![sourceDB](model/sourceDB.png)


**Diagrama dw:**
![dw](model/dw.png)

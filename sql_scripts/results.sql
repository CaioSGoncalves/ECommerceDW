-- Número de Itens de Pedido Por Dia.
USE sourceDB;
SELECT COUNT(0), DATE_FORMAT(orders.order_purchase_timestamp, "%Y/%m/%d") date
FROM order_item
INNER JOIN orders on order_item.order_id = orders.order_id
GROUP BY date
ORDER BY date;
# 0.364 sec / 0.000055 sec

USE dw;
SELECT COUNT(0), dim_date.str as date
FROM fact_order_item
INNER JOIN dim_date on dim_date.id = fact_order_item.purchase_timestamp_date_id
GROUP BY date
ORDER BY date;
# 0.179 sec / 0.00010 sec

-- Número de Itens de Pedido por Localidade.
USE sourceDB;
SELECT COUNT(0), UPPER(customer.customer_state) as state, UPPER(customer.customer_city) as city
FROM order_item
INNER JOIN orders on order_item.order_id = orders.order_id
INNER JOIN customer on customer.customer_id = orders.customer_id
GROUP BY state, city;
-- 0.501 sec / 0.0034 sec

USE dw;
SELECT COUNT(0), dim_local.state as state, dim_local.city as city
FROM fact_order_item
INNER JOIN dim_customer on dim_customer.id = fact_order_item.customer_id
INNER JOIN dim_local on dim_local.id = dim_customer.local_id
GROUP BY state, city;
-- 0.271 sec / 0.0045 sec

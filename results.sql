-- Número de Itens de Pedido Por Dia.
USE sourceDB;
SELECT COUNT(0), DATE_FORMAT(orders.order_purchase_timestamp, "%Y/%m/%d") date
FROM order_item
INNER JOIN orders on order_item.order_id = orders.order_id
GROUP BY date
ORDER BY date;

USE dw;
SELECT COUNT(0), dim_date.str as date
FROM fact_order_item
INNER JOIN dim_date on dim_date.id = fact_order_item.purchase_timestamp_date_id
GROUP BY date
ORDER BY date;

-- Número de Pedidos por Localidade.

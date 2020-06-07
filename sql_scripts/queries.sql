(SELECT
	customer_zip_code_prefix as zip_code_prefix,
    customer_city as city,
    customer_state as state
 FROM customer)
UNION
(SELECT 
	seller_zip_code_prefix as zip_code_prefix,
    seller_city as city,
    seller_state as state
 FROM seller);
 
SELECT
	product.product_id as original_id,
	product.product_category_name as category_name
FROM product;

SELECT * FROM dim_local;
SELECT * FROM dim_product;

SELECT
	order_id as order_id,
    payment_sequential as sequential,
    payment_type as type,
    payment_installments as installments,
    payment_value as value
FROM order_payment;

SELECT * 
FROM dim_order_payment
INNER JOIN dim_payment on dim_payment.order_payment_id = dim_order_payment.id;

SELECT 
	seller_id as original_id,
    seller_zip_code_prefix as zip_code_prefix
 FROM seller;
 
SELECT COUNT(0), local_id FROM dim_seller GROUP BY local_id;
SELECT COUNT(0), seller_zip_code_prefix FROM sourceDB.seller GROUP BY seller_zip_code_prefix;


SELECT 
	customer_id as original_id,
    customer_unique_id as unique_id,
    customer_zip_code_prefix as zip_code_prefix
 FROM customer;

(SELECT 
	DAY(order_purchase_timestamp) as day,
    MONTH(order_purchase_timestamp) as month,
    YEAR(order_purchase_timestamp) as year,
    DATE_FORMAT(order_purchase_timestamp, "%Y/%m/%d") as str
FROM orders WHERE order_purchase_timestamp IS NOT NULL)
UNION
(SELECT 
	DAY(order_approved_at) as day,
    MONTH(order_approved_at) as month,
    YEAR(order_approved_at) as year,
    DATE_FORMAT(order_approved_at, "%Y/%m/%d") as str
FROM orders WHERE order_approved_at IS NOT NULL)
UNION
(SELECT 
	DAY(order_delivered_carrier_date) as day,
    MONTH(order_delivered_carrier_date) as month,
    YEAR(order_delivered_carrier_date) as year,
    DATE_FORMAT(order_delivered_carrier_date, "%Y/%m/%d") as str
FROM orders WHERE order_delivered_carrier_date IS NOT NULL)
UNION
(SELECT 
	DAY(order_delivered_customer_date) as day,
    MONTH(order_delivered_customer_date) as month,
    YEAR(order_delivered_customer_date) as year,
    DATE_FORMAT(order_delivered_customer_date, "%Y/%m/%d") as str
FROM orders WHERE order_delivered_customer_date IS NOT NULL)
UNION
(SELECT 
	DAY(order_estimated_delivery_date) as day,
    MONTH(order_estimated_delivery_date) as month,
    YEAR(order_estimated_delivery_date) as year,
    DATE_FORMAT(order_estimated_delivery_date, "%Y/%m/%d") as str
FROM orders WHERE order_estimated_delivery_date IS NOT NULL);

SELECT * FROM dim_date;

SELECT 
	order_item.order_item_id as original_id,
	orders.order_id as order_id,
    orders.customer_id as customer_original_id,
    orders.order_status as status,
    
    DATE_FORMAT(orders.order_purchase_timestamp, "%Y/%m/%d") as purchase_timestamp_str,
	DATE_FORMAT(orders.order_approved_at, "%Y/%m/%d") as approved_at_str,
	DATE_FORMAT(orders.order_delivered_carrier_date, "%Y/%m/%d") as delivered_carrier_date_str,
	DATE_FORMAT(orders.order_delivered_customer_date, "%Y/%m/%d") as delivered_customer_date_str,
	DATE_FORMAT(orders.order_estimated_delivery_date, "%Y/%m/%d") as estimated_delivery_date_str,
    
    order_item.price as price,
    order_item.product_id as product_original_id,
    order_item.seller_id as seller_original_id
 FROM orders
 INNER JOIN order_item on order_item.order_id = orders.order_id;
 
 SELECT * FROM fact_order_item;


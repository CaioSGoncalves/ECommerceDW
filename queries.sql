SELECT * FROM customer;

SELECT * FROM orders;

SELECT orders.order_id, orders.order_status, customer.customer_id, customer.customer_state 
FROM orders
INNER JOIN customer on customer.customer_id = orders.customer_id;


SELECT * FROM product;


-- CREATE DATABASE dw;
-- USE dw;

DROP TABLE IF EXISTS fact_review;
DROP TABLE IF EXISTS fact_order_item;

DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_seller;

DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS dim_order_payment;

DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_local;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
	id INT(11) NOT NULL AUTO_INCREMENT,
    day INT(2) NOT NULL,
    month INT(2) NOT NULL,
    year INT(4) NOT NULL,
    str VARCHAR(255) NOT NULL,
    
    PRIMARY KEY (id)
);

CREATE TABLE dim_local (
	id INT(11) NOT NULL AUTO_INCREMENT,
    zip_code_prefix INT(10) NOT NULL,
    state VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    
    PRIMARY KEY (id)
);

CREATE TABLE dim_product (
	id INT(11) NOT NULL AUTO_INCREMENT,
    original_id VARCHAR(32) NOT NULL,
    category_name VARCHAR(255) NULL,
    
    PRIMARY KEY (id)
);

CREATE TABLE dim_order_payment (
	id INT(11) NOT NULL AUTO_INCREMENT,
    order_id VARCHAR(32) NOT NULL,
    
    PRIMARY KEY (id)
);

CREATE TABLE dim_payment (
	id INT(11) NOT NULL AUTO_INCREMENT,
    order_payment_id INT(11) NOT NULL,
    
    sequential INT(2) NOT NULL,
    type VARCHAR(255) NOT NULL,
    installments INT(2) NOT NULL,
	value FLOAT NULL,
    
    FOREIGN KEY (order_payment_id) REFERENCES dim_order_payment(id),
    PRIMARY KEY (id)
);

CREATE TABLE dim_seller (
	id INT(11) NOT NULL AUTO_INCREMENT,
    local_id INT(11) NOT NULL,
    original_id VARCHAR(32) NOT NULL,
	
    FOREIGN KEY (local_id) REFERENCES dim_local(id),
    PRIMARY KEY (id)
);

CREATE TABLE dim_customer (
	id INT(11) NOT NULL AUTO_INCREMENT,
    original_id VARCHAR(32) NOT NULL,
    unique_id VARCHAR(32) NOT NULL,
    local_id INT(11) NOT NULL,
	
    FOREIGN KEY (local_id) REFERENCES dim_local(id),
    PRIMARY KEY (id)
);

CREATE TABLE fact_order_item (
	id INT(11) NOT NULL AUTO_INCREMENT,
    order_id VARCHAR(32) NOT NULL,
    product_id INT(11) NOT NULL,
    seller_id INT(11) NOT NULL,
    customer_id INT(11) NOT NULL,
    original_id VARCHAR(32) NOT NULL,
    order_payment_id INT(11) NULL,
    
    price FLOAT NOT NULL,
	status VARCHAR(255) NOT NULL,
    purchase_timestamp_date_id INT(11) NULL,
    approved_at_date_id INT(11) NULL,
    delivered_carrier_date_id INT(11) NULL,
    delivered_customer_date_id INT(11) NULL,
    estimated_delivery_date_id INT(11) NULL,
	
    FOREIGN KEY (product_id) REFERENCES dim_product(id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller(id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(id),
    FOREIGN KEY (order_payment_id) REFERENCES dim_order_payment(id),
    FOREIGN KEY (purchase_timestamp_date_id) REFERENCES dim_date(id),
    FOREIGN KEY (approved_at_date_id) REFERENCES dim_date(id),
    FOREIGN KEY (delivered_carrier_date_id) REFERENCES dim_date(id),
    FOREIGN KEY (delivered_customer_date_id) REFERENCES dim_date(id),
    FOREIGN KEY (estimated_delivery_date_id) REFERENCES dim_date(id),
    PRIMARY KEY (id)
);

CREATE TABLE fact_review (
	id INT(11) NOT NULL AUTO_INCREMENT,
    order_id INT(2) NOT NULL,
    order_item_id INT(2) NOT NULL,
    
    score INT(2) NOT NULL,
    comment_title VARCHAR(255) NULL,
    comment_message VARCHAR(255) NULL,
    creation_date TIMESTAMP NULL,
    answer_timestamp TIMESTAMP NULL,
    
    FOREIGN KEY (order_item_id) REFERENCES fact_order_item(id),
    PRIMARY KEY (id)
);
CREATE DATABASE sourceDB;
USE sourceDB;

DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS review;
DROP TABLE IF EXISTS order_item;
DROP TABLE IF EXISTS order_payment;

DROP TABLE IF EXISTS orders;

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS seller;


CREATE TABLE customer (
	customer_id VARCHAR(32) NOT NULL,
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix INT(11) NOT NULL,
    customer_city VARCHAR(255) NOT NULL,
    customer_state VARCHAR(255) NOT NULL,

    PRIMARY KEY (customer_id)
);

CREATE TABLE orders (
	order_id VARCHAR(32) NOT NULL,
    customer_id VARCHAR(32) NOT NULL,
    order_status VARCHAR(255) NOT NULL,
    order_purchase_timestamp TIMESTAMP NULL,
    order_approved_at TIMESTAMP NULL,
    order_delivered_carrier_date TIMESTAMP NULL,
    order_delivered_customer_date TIMESTAMP NULL,
    order_estimated_delivery_date TIMESTAMP NULL,
	
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    PRIMARY KEY (order_id)
);

CREATE TABLE product (
	product_id VARCHAR(32) NOT NULL,
    product_category_name VARCHAR(255) NULL,
    product_name_lenght FLOAT NULL,
    product_description_lenght FLOAT NULL,
    product_photos_qty FLOAT NULL,
    product_weight_g FLOAT NULL,
    product_length_cm FLOAT NULL,
	product_height_cm FLOAT NULL,
    product_width_cm FLOAT NULL,
    
    PRIMARY KEY (product_id)
);

CREATE TABLE seller (
	seller_id VARCHAR(32) NOT NULL,
    seller_zip_code_prefix INT(10) NOT NULL,
    seller_city VARCHAR(255) NOT NULL,
    seller_state VARCHAR(255) NOT NULL,
	
    PRIMARY KEY (seller_id)
);

CREATE TABLE review (
	review_id VARCHAR(32) NOT NULL,
    order_id VARCHAR(32) NOT NULL,
    review_score INT(2) NOT NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message VARCHAR(255) NULL,
    review_creation_date TIMESTAMP NULL,
    review_answer_timestamp TIMESTAMP NULL,
    
	FOREIGN KEY (order_id) REFERENCES orders(order_id),
    PRIMARY KEY (review_id)
);

CREATE TABLE order_item (
    order_id VARCHAR(32) NOT NULL,
	order_item_id VARCHAR(32) NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    seller_id VARCHAR(32) NOT NULL,
    shipping_limit_date TIMESTAMP NULL,
	price FLOAT NULL,
    freight_value FLOAT NULL,
    
	FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payment (
    order_id VARCHAR(32) NOT NULL,
	payment_sequential INT(2) NOT NULL,
    payment_type VARCHAR(255) NOT NULL,
    payment_installments INT(2) NOT NULL,
	payment_value FLOAT NULL,
    
	FOREIGN KEY (order_id) REFERENCES orders(order_id),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE geolocation (
    geolocation_zip_code_prefix INT(10) NOT NULL,
	geolocation_lat FLOAT NOT NULL,
    geolocation_lng FLOAT NOT NULL,
    geolocation_city VARCHAR(255) NOT NULL,
	geolocation_state VARCHAR(255) NOT NULL
);
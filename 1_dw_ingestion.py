import re

import pandas as pd
from sqlalchemy import create_engine


def remove_special_chars(x):
    return re.sub(r"[^a-zA-Z0-9]+", ' ', x)


def transform_str(value):
    if type(value) is not str:
        return None
    new = remove_special_chars(value)
    return new.upper()


def ingest_dim_local(source_engine, dw_engine):
    source_connection = source_engine.connect()
    table_name = "dim_local"

    results = source_connection.execute("""
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

    """)
    data = pd.DataFrame(results.fetchall())
    data.columns = results.keys()

    data.drop_duplicates(subset=['zip_code_prefix'], keep='first', inplace=True)
    data["city"] = data["city"].map(transform_str)
    data["state"] = data["state"].map(transform_str)

    data.to_sql(name=table_name, con=dw_engine, if_exists='append', index=False)


def ingest_dim_product(source_engine, dw_engine):
    source_connection = source_engine.connect()
    table_name = "dim_product"

    results = source_connection.execute("""
        SELECT
            product.product_id as original_id,
            product.product_category_name as category_name
        FROM product;
    """)
    data = pd.DataFrame(results.fetchall())
    data.columns = results.keys()

    data["category_name"] = data["category_name"].map(transform_str)

    data.to_sql(name=table_name, con=dw_engine, if_exists='append', index=False)


def ingest_dim_order_payment_and_dim_payment(source_engine, dw_engine):
    source_connection = source_engine.connect()

    results = source_connection.execute("""
        SELECT
            order_id as order_id,
            payment_sequential as sequential,
            payment_type as type,
            payment_installments as installments,
            payment_value as value
        FROM order_payment;
    """)
    data = pd.DataFrame(results.fetchall())
    data.columns = results.keys()

    order_payment = data[['order_id']].copy()
    order_payment.drop_duplicates(subset=['order_id'], keep='first', inplace=True)
    order_payment.to_sql(name="dim_order_payment", con=dw_engine, if_exists='append', index=False)
    order_payment = pd.read_sql_table("dim_order_payment", con=dw_engine)

    data = pd.merge(data, order_payment, on='order_id')
    data.rename({'id': 'order_payment_id'}, axis=1, inplace=True)
    payment = data[['order_payment_id', 'sequential', 'type', 'installments', 'value']].copy()
    payment.to_sql(name="dim_payment", con=dw_engine, if_exists='append', index=False)


def ingest_dim_seller(source_engine, dw_engine):
    source_connection = source_engine.connect()
    table_name = "dim_seller"

    results = source_connection.execute("""
        SELECT 
            seller_id as original_id,
            seller_zip_code_prefix as zip_code_prefix
        FROM seller;
        """)
    seller = pd.DataFrame(results.fetchall())
    seller.columns = results.keys()

    dim_local = pd.read_sql_table("dim_local", con=dw_engine)

    data = pd.merge(seller, dim_local, on='zip_code_prefix')
    data.rename({'id': 'local_id'}, axis=1, inplace=True)
    dim_seller = data[['original_id', 'local_id']].copy()
    dim_seller.to_sql(name=table_name, con=dw_engine, if_exists='append', index=False)


def ingest_dim_customer(source_engine, dw_engine):
    source_connection = source_engine.connect()
    table_name = "dim_customer"

    results = source_connection.execute("""
    SELECT 
        customer_id as original_id,
        customer_unique_id as unique_id,
        customer_zip_code_prefix as zip_code_prefix
    FROM customer;
        """)
    customer = pd.DataFrame(results.fetchall())
    customer.columns = results.keys()

    dim_local = pd.read_sql_table("dim_local", con=dw_engine)

    data = pd.merge(customer, dim_local, on='zip_code_prefix')
    data.rename({'id': 'local_id'}, axis=1, inplace=True)
    dim_seller = data[['original_id', 'local_id', 'unique_id']].copy()
    dim_seller.to_sql(name=table_name, con=dw_engine, if_exists='append', index=False)


def ingest_dim_date(source_engine, dw_engine):
    source_connection = source_engine.connect()
    table_name = "dim_date"

    results = source_connection.execute("""
    (SELECT 
        DAY(order_purchase_timestamp) as day,
        MONTH(order_purchase_timestamp) as month,
        YEAR(order_purchase_timestamp) as year,
        DATE_FORMAT(order_purchase_timestamp, "%%Y/%%m/%%d") as str
    FROM orders WHERE order_purchase_timestamp IS NOT NULL)
    UNION
    (SELECT 
        DAY(order_approved_at) as day,
        MONTH(order_approved_at) as month,
        YEAR(order_approved_at) as year,
        DATE_FORMAT(order_approved_at, "%%Y/%%m/%%d") as str
    FROM orders WHERE order_approved_at IS NOT NULL)
    UNION
    (SELECT 
        DAY(order_delivered_carrier_date) as day,
        MONTH(order_delivered_carrier_date) as month,
        YEAR(order_delivered_carrier_date) as year,
        DATE_FORMAT(order_delivered_carrier_date, "%%Y/%%m/%%d") as str
    FROM orders WHERE order_delivered_carrier_date IS NOT NULL)
    UNION
    (SELECT 
        DAY(order_delivered_customer_date) as day,
        MONTH(order_delivered_customer_date) as month,
        YEAR(order_delivered_customer_date) as year,
        DATE_FORMAT(order_delivered_customer_date, "%%Y/%%m/%%d") as str
    FROM orders WHERE order_delivered_customer_date IS NOT NULL)
    UNION
    (SELECT 
        DAY(order_estimated_delivery_date) as day,
        MONTH(order_estimated_delivery_date) as month,
        YEAR(order_estimated_delivery_date) as year,
        DATE_FORMAT(order_estimated_delivery_date, "%%Y/%%m/%%d") as str
    FROM orders WHERE order_estimated_delivery_date IS NOT NULL);
        """)
    data = pd.DataFrame(results.fetchall())
    data.columns = results.keys()

    data.drop_duplicates(subset=['str'], keep='first', inplace=True)
    data.to_sql(name=table_name, con=dw_engine, if_exists='append', index=False)


def ingest_dims():
    source_engine = create_engine("mysql+pymysql://root:12345@localhost:3307/sourceDB", echo=False)
    dw_engine = create_engine("mysql+pymysql://root:12345@localhost:3307/dw", echo=False)

    ingest_dim_date(source_engine, dw_engine)
    ingest_dim_local(source_engine, dw_engine)
    ingest_dim_product(source_engine, dw_engine)
    ingest_dim_order_payment_and_dim_payment(source_engine, dw_engine)
    ingest_dim_seller(source_engine, dw_engine)
    ingest_dim_customer(source_engine, dw_engine)


def ingest_fact_order_item(source_engine, dw_engine):
    source_connection = source_engine.connect()
    table_name = "fact_order_item"

    results = source_connection.execute("""
     SELECT 
        order_item.order_item_id as original_id,
        orders.order_id as order_id,
        orders.customer_id as customer_original_id,
        orders.order_status as status,
        
        DATE_FORMAT(orders.order_purchase_timestamp, "%%Y/%%m/%%d") as purchase_timestamp_str,
        DATE_FORMAT(orders.order_approved_at, "%%Y/%%m/%%d") as approved_at_str,
        DATE_FORMAT(orders.order_delivered_carrier_date, "%%Y/%%m/%%d") as delivered_carrier_date_str,
        DATE_FORMAT(orders.order_delivered_customer_date, "%%Y/%%m/%%d") as delivered_customer_date_str,
        DATE_FORMAT(orders.order_estimated_delivery_date, "%%Y/%%m/%%d") as estimated_delivery_date_str,
        
        order_item.price as price,
        order_item.product_id as product_original_id,
        order_item.seller_id as seller_original_id
     FROM orders
     INNER JOIN order_item on order_item.order_id = orders.order_id;
        """)
    data = pd.DataFrame(results.fetchall())
    data.columns = results.keys()

    dim_date = pd.read_sql_table("dim_date", con=dw_engine)
    dim_customer = pd.read_sql_table("dim_customer", con=dw_engine)
    dim_product = pd.read_sql_table("dim_product", con=dw_engine)
    dim_seller = pd.read_sql_table("dim_seller", con=dw_engine)
    dim_order_payment = pd.read_sql_table("dim_order_payment", con=dw_engine)

    data = pd.merge(data, dim_customer, left_on=['customer_original_id'], right_on=['original_id'])
    data.rename({'id': 'customer_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_product, left_on=['product_original_id'], right_on=['original_id'])
    data.rename({'id': 'product_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_seller, left_on=['seller_original_id'], right_on=['original_id'])
    data.rename({'id': 'seller_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_date, how='left', left_on=['purchase_timestamp_str'], right_on=['str'])
    data.rename({'id': 'purchase_timestamp_date_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_date, how='left', left_on=['approved_at_str'], right_on=['str']) #
    data.rename({'id': 'approved_at_date_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_date, how='left', left_on=['delivered_carrier_date_str'], right_on=['str']) #
    data.rename({'id': 'delivered_carrier_date_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_date, how='left', left_on=['delivered_customer_date_str'], right_on=['str']) #
    data.rename({'id': 'delivered_customer_date_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_date, how='left', left_on=['estimated_delivery_date_str'], right_on=['str'])
    data.rename({'id': 'estimated_delivery_date_id'}, axis=1, inplace=True)

    data = pd.merge(data, dim_order_payment, how='left', on='order_id') #
    data.rename({'id': 'order_payment_id'}, axis=1, inplace=True)

    data.rename({'original_id_x': 'original_id'}, axis=1, inplace=True)
    fact_order_item = data[
        ['order_id', 'product_id', 'seller_id', 'customer_id', 'original_id', 'order_payment_id', 'price', 'status',
         'purchase_timestamp_date_id', 'approved_at_date_id', 'delivered_carrier_date_id', 'delivered_customer_date_id',
         'estimated_delivery_date_id']].copy()
    fact_order_item.to_sql(name=table_name, con=dw_engine, if_exists='append', index=False)


def ingest_facts():
    source_engine = create_engine("mysql+pymysql://root:12345@localhost:3307/sourceDB", echo=False)
    dw_engine = create_engine("mysql+pymysql://root:12345@localhost:3307/dw", echo=False)

    ingest_fact_order_item(source_engine, dw_engine)


def delete_all():
    dw_engine = create_engine("mysql+pymysql://root:12345@localhost:3307/dw", echo=False)
    dw_connection = dw_engine.connect()
    dw_connection.execute("DELETE FROM fact_order_item;")
    dw_connection.execute("DELETE FROM dim_customer;")
    dw_connection.execute("DELETE FROM dim_seller;")
    dw_connection.execute("DELETE FROM dim_payment;")
    dw_connection.execute("DELETE FROM dim_order_payment;")
    dw_connection.execute("DELETE FROM dim_product;")
    dw_connection.execute("DELETE FROM dim_local;")
    dw_connection.execute("DELETE FROM dim_date;")


if __name__ == "__main__":
    delete_all()
    ingest_dims()
    ingest_facts()

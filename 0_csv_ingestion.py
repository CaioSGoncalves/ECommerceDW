import re

import pandas as pd
from sqlalchemy import create_engine


def remove_special_chars(x):
    return re.sub(r"[^a-zA-Z0-9]+", ' ', x) if type(x) is str else None


def ingest_into_source_db():
    engine = create_engine("mysql+pymysql://root:12345@localhost:3307/sourceDB", echo=False)

    customers = pd.read_csv("./data/olist_customers_dataset.csv")
    customers.to_sql(name="customer", con=engine, if_exists='append', index=False)

    orders = pd.read_csv("./data/olist_orders_dataset.csv")
    orders.to_sql(name="orders", con=engine, if_exists='append', index=False)

    products = pd.read_csv("./data/olist_products_dataset.csv")
    products.to_sql(name="product", con=engine, if_exists='append', index=False)

    sellers = pd.read_csv("./data/olist_sellers_dataset.csv")
    sellers["seller_city"] = sellers["seller_city"].map(remove_special_chars)
    sellers.to_sql(name="seller", con=engine, if_exists='append', index=False)

    reviews = pd.read_csv("./data/olist_order_reviews_dataset.csv")
    reviews["review_comment_message"] = reviews["review_comment_message"].map(remove_special_chars)
    reviews["review_comment_title"] = reviews["review_comment_title"].map(remove_special_chars)
    reviews.drop_duplicates(subset=['review_id'], keep='first', inplace=True)
    reviews.to_sql(name="review", con=engine, if_exists='append', index=False)

    order_items = pd.read_csv("./data/olist_order_items_dataset.csv")
    order_items.to_sql(name="order_item", con=engine, if_exists='append', index=False)

    order_payments = pd.read_csv("./data/olist_order_payments_dataset.csv")
    order_payments.to_sql(name="order_payment", con=engine, if_exists='append', index=False)

    # geolocation = pd.read_csv("./data/olist_geolocation_dataset.csv")
    # geolocation.to_sql(name="geolocation", con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    ingest_into_source_db()

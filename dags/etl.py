import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy import text
import random

from airflow import DAG
from airflow.operators.python import PythonOperator

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.5195.125 Safari/537.36",
]

header = {
    "accept": "*/*",
    "origin": "https://www.sendo.vn",
    "referer": "https://www.sendo.vn/",
    "user-agent": random.choice(USER_AGENTS),
}


def get_sub_category_list(ti):
    reponse = requests.get(
        "https://mapi.sendo.vn/wap_v2/category/sitemap",
        headers=header,
    )
    content = reponse.json()
    list_cartegory = content["result"]["data"]
    sub_category_dict = {}
    for cartegory in list_cartegory:
        temp_list = []
        for sub_category in cartegory["child"]:
            sub_category_url_key = sub_category["url_key"]
            temp_list.append(sub_category_url_key)
        category_url_key = cartegory["url_key"]
        sub_category_dict[category_url_key] = temp_list
    ti.xcom_push(key="sub_category_dict", value=sub_category_dict)


def get_product(ti):
    message = ti.xcom_pull(
        task_ids="task_get_sub_category_dict", key="sub_category_dict"
    )
    all_product = []
    for category, sub_categories in message.items():
        for sub_category in sub_categories:
            page = 1
            while True:
                reponse = requests.get(
                    f"https://searchlist-api.sendo.vn/app/products?category_path={sub_category}&page={page}",
                    headers=header,
                )
                content = reponse.json()
                data = content["data"]
                if data is None:
                    break
                df = pd.DataFrame.from_dict(data)
                df["category"] = category
                df["sub_category"] = sub_category
                all_product.append(df)
                page += 1
            print(f"Đã quét subcategory: {sub_category} của category: {category}")
    all_product = pd.concat(all_product, ignore_index=True)
    all_product = all_product[
        [
            "product_id",
            "name",
            "category_path",
            "price",
            "price_max",
            "final_price",
            "final_price_max",
            "shop_id",
            "category",
            "sub_category",
        ]
    ]
    ti.xcom_push(key="all_product", value=all_product)


def get_shop_info(ti):
    message = ti.xcom_pull(task_ids="task_get_product", key="all_product")
    message = message.drop_duplicates(subset=["shop_id"])
    products = message["category_path"]
    all_shop_info = []
    for product in products:
        reponse = requests.get(
            f"https://detail-api.sendo.vn/full/{product.replace('.html', '')}",
            headers=header,
        )
        content = reponse.json()
        shop_info = content["data"]["shop_info"]
        all_shop_info.append(shop_info)
    all_shop_info = pd.DataFrame.from_dict(all_shop_info)
    all_shop_info = all_shop_info[
        [
            "shop_id",
            "shop_name",
            "good_review_percent",
            "score",
            "customer_id",
            "phone_number",
            "rating_avg",
            "rating_count",
            "response_time",
            "product_total",
            "sale_on_sendo",
            "time_prepare_product",
            "warehourse_region_name",
        ]
    ]
    total_parts = 5
    part_size = len(all_shop_info) // total_parts
    parts = [
        all_shop_info.iloc[i * part_size : (i + 1) * part_size]
        for i in range(total_parts)
    ]
    ti.xcom_push(key="all_shop_info", value=all_shop_info)
    ti.xcom_push(key="all_shop_info_parts", value=parts)


def get_rating(ti, part):
    message = ti.xcom_pull(task_ids="task_get_shop_info", key="all_shop_info_parts")[
        part
    ]
    shop_ids = message["shop_id"]
    all_rating = []
    for shop_id in shop_ids:
        page = 1
        while True:
            reponse = requests.get(
                f"https://shop-home.sendo.vn/api/web/v1/shop/rating/{shop_id}?page={page}&limit=10000",
                headers=header,
            )
            content = reponse.json()
            ratings = content["data"]["ratings"]
            if len(ratings) == 0:
                break
            df = pd.DataFrame.from_dict(ratings)
            df["shop_id"] = shop_id
            all_rating.append(df)
            page += 1
        print(f"Đã lấy đánh giá của {shop_id}")
    all_rating = pd.concat(all_rating, ignore_index=True)
    all_rating = all_rating[
        [
            "rating_id",
            "shop_id",
            "address",
            "star",
            "comment",
            "status",
            "update_time",
            "customer_id",
            "user_name",
            "product_name",
            "product_path",
            "price",
        ]
    ]
    all_rating["update_time"] = pd.to_datetime(
        all_rating["update_time"], format="%d/%m/%Y"
    )
    ti.xcom_push(key="all_rating", value=all_rating)


def insert_rating_into_db(ti, part):
    rating = ti.xcom_pull(task_ids=f"task_get_rating_{part}", key="all_rating")
    engine = create_engine("mysql+mysqlconnector://sendo:sendo@mysql-sendo/sendo")
    rating.to_sql(
        f"temp_rating_{part}",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )
    merge_rating_sql = text(f"""
        INSERT INTO rating (rating_id, shop_id, address, star, comment, status, update_time, 
                            customer_id, user_name, product_name, product_path, price)
        SELECT rating_id, shop_id, address, star, comment, status, update_time, 
               customer_id, user_name, product_name, product_path, price
        FROM temp_rating_{part}
        ON DUPLICATE KEY UPDATE
            shop_id = VALUES(shop_id),
            address = VALUES(address),
            star = VALUES(star),
            comment = VALUES(comment),
            status = VALUES(status),
            update_time = VALUES(update_time),
            customer_id = VALUES(customer_id),
            user_name = VALUES(user_name),
            product_name = VALUES(product_name),
            product_path = VALUES(product_path),
            price = VALUES(price);
    """)
    with engine.connect() as connection:
        connection.execute(merge_rating_sql)
        connection.execute(text(f"DROP TABLE IF EXISTS temp_rating_{part}"))
    print("Đã thêm rating và database")


def insert_product_and_shop_into_db(ti):
    shop_info = ti.xcom_pull(task_ids="task_get_shop_info", key="all_shop_info")
    product_detail = ti.xcom_pull(task_ids="task_get_product", key="all_product")
    engine = create_engine("mysql+mysqlconnector://sendo:sendo@mysql-sendo/sendo")
    shop_info.to_sql(
        "temp_shop_info",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )
    merge_shop_sql = text("""
        INSERT INTO shop_info (shop_id, shop_name, good_review_percent, score, customer_id,
                                phone_number, rating_avg, rating_count, response_time,
                                product_total, sale_on_sendo, time_prepare_product,
                                warehourse_region_name)
        SELECT shop_id, shop_name, good_review_percent, score, customer_id,
                phone_number, rating_avg, rating_count, response_time,
                product_total, sale_on_sendo, time_prepare_product,
                warehourse_region_name
        FROM temp_shop_info
        ON DUPLICATE KEY UPDATE
            shop_name = VALUES(shop_name),
            good_review_percent = VALUES(good_review_percent),
            score = VALUES(score),
            customer_id = VALUES(customer_id),
            phone_number = VALUES(phone_number),
            rating_avg = VALUES(rating_avg),
            rating_count = VALUES(rating_count),
            response_time = VALUES(response_time),
            product_total = VALUES(product_total),
            sale_on_sendo = VALUES(sale_on_sendo),
            time_prepare_product = VALUES(time_prepare_product),
            warehourse_region_name = VALUES(warehourse_region_name);
    """)

    with engine.connect() as connection:
        connection.execute(merge_shop_sql)
        connection.execute(text("DROP TABLE IF EXISTS temp_shop_info"))
    print("Đã thêm shop_info và database")
    query = text("SELECT shop_id from shop_info")
    with engine.connect() as connection:
        shop_id_from_db = pd.read_sql(query, connection)
    all_shop_id = set(shop_id_from_db["shop_id"]).union(set(shop_info["shop_id"]))
    product_detail = product_detail[product_detail["shop_id"].isin(all_shop_id)]
    product_detail.to_sql(
        "temp_product_detail",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )
    merge_product_sql = text("""
        INSERT INTO product_detail (product_id, name, category_path, price, price_max, 
                                    final_price, final_price_max, shop_id, category, sub_category)
        SELECT product_id, name, category_path, price, price_max, 
               final_price, final_price_max, shop_id, category, sub_category
        FROM temp_product_detail
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            category_path = VALUES(category_path),
            price = VALUES(price),
            price_max = VALUES(price_max),
            final_price = VALUES(final_price),
            final_price_max = VALUES(final_price_max),
            shop_id = VALUES(shop_id),
            category = VALUES(category),
            sub_category = VALUES(sub_category);
    """)

    with engine.connect() as connection:
        connection.execute(merge_product_sql)
        connection.execute(text("DROP TABLE IF EXISTS temp_product_detail"))
    print("Đã thêm product_detail và database")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.combine(datetime.today(), datetime.min.time()),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_sendo_webscraping",
    default_args=default_args,
    description="ETL để cào dữ liệu sản phẩm, thông tin cửa hàng và đánh giá của cửa hàng",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task_get_sub_category_dict = PythonOperator(
        task_id="task_get_sub_category_dict", python_callable=get_sub_category_list
    )
    task_get_product = PythonOperator(
        task_id="task_get_product", python_callable=get_product
    )
    task_get_shop_info = PythonOperator(
        task_id="task_get_shop_info", python_callable=get_shop_info
    )
    task_get_rating_list = []
    for i in range(5):
        task_get_rating = PythonOperator(
            task_id=f"task_get_rating_{i}",
            python_callable=get_rating,
            op_kwargs={"part": i},
        )
        task_get_rating_list.append(task_get_rating)
    task_insert_rating_into_db_list = []
    for i in range(5):
        task_insert_rating_into_db = PythonOperator(
            task_id=f"task_insert_rating_into_db_{i}",
            python_callable=insert_rating_into_db,
            op_kwargs={"part": i},
        )
        task_insert_rating_into_db_list.append(task_insert_rating_into_db)
    task_insert_product_and_shop_into_db = PythonOperator(
        task_id="task_insert_product_and_shop_into_db",
        python_callable=insert_product_and_shop_into_db,
    )

    (
        task_get_sub_category_dict
        >> task_get_product
        >> task_get_shop_info
        >> task_get_rating_list
    )
    for task_get_rating, task_insert_rating_into_db in zip(
        task_get_rating_list, task_insert_rating_into_db_list
    ):
        task_get_rating >> task_insert_rating_into_db

    [
        task_get_shop_info,
        task_get_product,
    ] >> task_insert_product_and_shop_into_db

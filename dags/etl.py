import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator


header = {
    "accept": "*/*",
    "origin": "https://www.sendo.vn",
    "referer": "https://www.sendo.vn/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
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
    all_product = pd.DataFrame()
    for category, sub_categories in message.items():
        for sub_category in sub_categories:
            page = 1
            while True:
                time.sleep(random.randint(2, 4))
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
                all_product = pd.concat([all_product, df], ignore_index=True)
                print(
                    f"Đã quét subcategory: {sub_category} của category: {category} tại page: {page}"
                )
                page += 1
            break
        break
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
        ]
    ]
    ti.xcom_push(key="all_product", value=all_product)


def get_shop_info(ti):
    message = ti.xcom_pull(task_ids="task_get_product", key="all_product")
    products = message["category_path"]
    all_product_expand = pd.DataFrame()
    product_key_needed = ["id", "short_description", "quantity"]
    all_shop_info = pd.DataFrame()
    for product in products:
        reponse = requests.get(
            f"https://detail-api.sendo.vn/full/{product.replace('.html', '')}",
            headers=header,
        )
        content = reponse.json()
        data = content["data"]
        shop_info = content["data"]["shop_info"]
        product_expand = {k: data[k] for k in product_key_needed if k in data}
        df_product_expand = pd.DataFrame.from_dict([product_expand])
        df_shop_info = pd.DataFrame.from_dict([shop_info])
        all_shop_info = pd.concat([all_shop_info, df_shop_info], ignore_index=True)
        all_product_expand = pd.concat(
            [all_product_expand, df_product_expand], ignore_index=True
        )
    all_shop_info = all_shop_info.drop_duplicates(subset=["shop_id"])
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
    ti.xcom_push(key="all_shop_info", value=all_shop_info)
    ti.xcom_push(key="all_product_expand", value=all_product_expand)


def final_product_detail(ti):
    message = ti.xcom_pull(task_ids="task_get_product", key="all_product")
    message1 = ti.xcom_pull(task_ids="task_get_shop_info", key="all_product_expand")

    final_product_detail = pd.merge(
        message, message1, left_on="product_id", right_on="id", how="inner"
    )
    ti.xcom_push(key="final_product_detail", value=final_product_detail)


def get_rating(ti):
    message = ti.xcom_pull(task_ids="task_get_shop_info", key="all_shop_info")
    shop_ids = message["shop_id"]
    all_rating = pd.DataFrame()
    for shop_id in shop_ids:
        page = 1
        while True:
            reponse = requests.get(
                f"https://shop-home.sendo.vn/api/web/v1/shop/rating/{shop_id}?page={page}&limit=1000",
                headers=header,
            )
            content = reponse.json()
            ratings = content["data"]["ratings"]
            if len(ratings) == 0:
                break
            df = pd.DataFrame.from_dict(ratings)
            df["shop_id"] = shop_id
            all_rating = pd.concat([all_rating, df], ignore_index=True)
            page += 1
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
    ti.xcom_push(key="all_rating", value=all_rating)


def insert_into_db(ti):
    pass


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.combine(datetime.today(), datetime.min.time()),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_tiki_webscraping",
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
    task_final_product_detail = PythonOperator(
        task_id="task_final_product_detail", python_callable=final_product_detail
    )
    task_get_rating = PythonOperator(
        task_id="task_get_rating", python_callable=get_rating
    )

    (
        task_get_sub_category_dict
        >> task_get_product
        >> task_get_shop_info
        >> task_get_rating
    )
    [task_get_product, task_get_shop_info] >> task_final_product_detail

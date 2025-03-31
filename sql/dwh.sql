CREATE TABLE shop_info (
    shop_id VARCHAR(20) PRIMARY KEY,
    shop_name TEXT,
    good_review_percent DECIMAL(10,2),
    score DECIMAL(10,2),
    customer_id VARCHAR(20),
    phone_number VARCHAR(20),
    rating_avg DECIMAL(10,2),
    rating_count INT,
    response_time TEXT,
    product_total INT,
    sale_on_sendo TEXT,
    time_prepare_product TEXT,
    warehourse_region_name TEXT
);

CREATE TABLE product_detail (
    product_id VARCHAR(20) PRIMARY KEY,
    name TEXT,
    category_path TEXT,
    price DECIMAL(15,2),
    price_max DECIMAL(15,2),
    final_price DECIMAL(15,2),
    final_price_max DECIMAL(15,2),
    shop_id VARCHAR(20),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    FOREIGN KEY (shop_id) REFERENCES shop_info(shop_id)
);

CREATE TABLE rating (
    rating_id VARCHAR(20) PRIMARY KEY,
    shop_id VARCHAR(20),
    address TEXT,
    star INT,
    comment TEXT,
    status TEXT,
    update_time DATE,
    customer_id VARCHAR(20),
    user_name TEXT,
    product_name TEXT,
    product_path TEXT,
    price DECIMAL(15,2),
    FOREIGN KEY (shop_id) REFERENCES shop_info(shop_id)
);

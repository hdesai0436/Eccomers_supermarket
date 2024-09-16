create table `hive-spark.eccomer_data.dim_customer` (
  customer_id INT64,
  first_name STRING,
  last_name STRING,
  email STRING,
  state STRING,
  zipcode STRING
);


create table `hive-spark.eccomer_data.dim_prodcut_category` (
  category_id INT64,
  category_name STRING
);

create table `hive-spark.eccomer_data.dim_prodcut`(
  product_id int64,
  product_name string,
  category_id int64,
  price NUMERIC

);

create table `hive-spark.eccomer_data.dim_promotion` (
  promotion_id int64,
  promotion_name string,
  start_date DATE,
  end_date date
);

create table `hive-spark.eccomer_data.dim_store_location` (
  store_id int64,
  city string,
  state string,
  zip_code string

);

create table `hive-spark.eccomer_data.fact` (
  sale_id int64,
  order_id int64,
  product_id int64,
  customer_id int64,
  promotion_id int64,
  store_id int64,
  sales_amount NUMERIC,
  quantity int64,
  discount_amount NUMERIC,
  order_date DATE
)
PARTITION BY order_date
CLUSTER BY product_id, customer_id, store_id;





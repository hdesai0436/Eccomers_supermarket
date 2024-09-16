from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,FloatType,TimestampType,  StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col,count,when,trim,to_date, date_format, sum as F_sum
# from app_log.logger import setup_logger
# logger = setup_logger(__file__)
from datetime import datetime

current_date = datetime.now().strftime('%Y-%m-%d')

            # Define GCS file path with table name and current date

   
spark = SparkSession.builder \
.appName("Orders-Customers-Data-Analysis") \
.getOrCreate()

bucket_name =  "eccomer_supermarket"
bucket_name1 = "fact-eccomer"

        # logger.error('something went wrong while creating spark session {e}')
categories = f"gs://{bucket_name}/raw/{current_date}/categories/categories.csv"
customer = f"gs://{bucket_name}/raw/{current_date}/customer/customer.csv"
order_items = f"gs://{bucket_name}/raw/{current_date}/order_items/order_items.csv"
orders = f"gs://{bucket_name}/raw/{current_date}/order_items/order_items.csv"
orders = f"gs://{bucket_name}/raw/{current_date}/orders/orders.csv"
products = f"gs://{bucket_name}/raw/{current_date}/products/products.csv"
promotion_product = f"gs://{bucket_name}/raw/{current_date}/promotion_product/promotion_product.csv"
promotion = f"gs://{bucket_name}/raw/{current_date}/promotion/promotion.csv"
sales = f"gs://{bucket_name}/raw/{current_date}/sales/sales.csv"
store_locations = f"gs://{bucket_name}/raw/{current_date}/store_locations/store_locations.csv"

category_schema = StructType([
    StructField('category_id',IntegerType(),True),
    StructField('category_name',StringType(),True)
])
sale_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("promotion_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("sale_date", TimestampType(), True),
    StructField("payment_type", StringType(), True)
])

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),  # Zip code as String because it can contain leading zeros
    StructField("created_at", TimestampType(), True)
])

product_schema = StructType([
    StructField('product_id',IntegerType(),True),
    StructField('product_name',StringType(),True),
    StructField('category_id',IntegerType(),True),
    StructField('price',FloatType(),True)
                
])
promotion_schma = StructType([
    StructField("promotion_id", IntegerType(), True),
    StructField("promotion_name", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("discount_percentage", FloatType(), True)
])

promotion_product_schma = StructType([
    StructField("promotion_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
])

store_location_schma = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    
])

inventory_schma = StructType([
    StructField("inventory_id", IntegerType(), True),
    StructField("store_location_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity_available", IntegerType(), True),
    
])
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_type", StringType(), True),
    
])

order_item_schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    
])

def check_missing_values_spark(df):
    print(f'checking missing values for {df}')
# Create a DataFrame that contains the count of nulls for each column
    missing_value_df = df.select([F_sum(col(c).isNull().cast('int')).alias(c) for c in df.columns])
    missing_values = missing_value_df.collect()[0].asDict()
    missing_columns = {k:v for k,v in missing_values.items() if v > 0} 

    if missing_columns:
        print(f'columns with missing values in {df}: {missing_columns}')
    else:
        print('No missing values in {df}') 


def check_and_remove_duplicated_data(df):
    duplicates = df.groupBy(df.columns).count().filter("count > 1")

    if duplicates.count() > 0:
        duplicates.show()

        df_clean = df.dropDuplicates()
    else:
        df_clean = df
    return df_clean

def find_and_remove_out_rang_columns(df,column_rang):

    for column,(min_val,max_val) in column_rang.items():
        out_of_rang = df.filter((col(column)< min_val) | col(column) > max_val)

        if out_of_rang.count() > 0:
            df = df.filter((col(column) >= min_val) & (col(column) <= max_val))
        else:
            print('No out of range ')
    return df
            


            
      

category_df  = spark.read.csv(categories, header=True, schema=category_schema)
sale_df = spark.read.csv(sales,header=True,schema=sale_schema)
customer_df = spark.read.csv(customer,header=True,schema=customer_schema)
order_items = spark.read.csv(order_items,header=True,schema=order_item_schema)
order_df = spark.read.csv(orders,header=True,schema=orders_schema)
product_df = spark.read.csv(products,header=True,schema=product_schema)
promotion_prodcut_df = spark.read.csv(promotion_product,header=True,schema=promotion_product_schma)
promotion_df = spark.read.csv(promotion,header=True,schema=promotion_schma)
store_df = spark.read.csv(store_locations,header=True,schema=store_location_schma)

check_missing_values_spark(category_df)
check_missing_values_spark(sale_df)
check_missing_values_spark(customer_df)
check_missing_values_spark(order_items)
check_missing_values_spark(order_df)
check_missing_values_spark(product_df)
check_missing_values_spark(promotion_prodcut_df)
check_missing_values_spark(promotion_df)
check_missing_values_spark(store_df)

clean_category_df = check_and_remove_duplicated_data(category_df)
clean_sale_df = check_and_remove_duplicated_data(sale_df)
clean_customer_df = check_and_remove_duplicated_data(customer_df)
clean_order_items = check_and_remove_duplicated_data(order_items)
clean_order_df = check_and_remove_duplicated_data(order_df)
clean_product_df = check_and_remove_duplicated_data(product_df)
clean_promotion_prodcut = check_and_remove_duplicated_data(promotion_prodcut_df)
clean_promotion_df = check_and_remove_duplicated_data(promotion_df)
clean_store_df = check_and_remove_duplicated_data(store_df)

promotion_combined_df = clean_promotion_prodcut \
                        .join(clean_promotion_df, clean_promotion_prodcut.promotion_id == clean_promotion_df.promotion_id,'inner')\
                        .select(clean_promotion_prodcut.product_id,
                                clean_promotion_df.promotion_id,
                                clean_promotion_df.promotion_name,
                                clean_promotion_df.discount_percentage,
                                clean_promotion_df.start_date,
                                clean_promotion_df.end_date
                                )

fact_table_df = clean_sale_df \
                .join(clean_order_df,clean_sale_df.order_id == clean_order_df.order_id,'inner')\
                .join(clean_product_df,clean_sale_df.product_id == clean_product_df.product_id,'inner')\
                .join(clean_customer_df,clean_sale_df.customer_id == clean_customer_df.customer_id, 'inner')\
                .join(clean_store_df, clean_sale_df.store_id == clean_store_df.store_id, 'inner')\
                .join(
                    promotion_combined_df.alias("promo"),
                    (clean_sale_df.product_id == col('promo.product_id')) &
                    (clean_sale_df.sale_date.between(col("promo.start_date"), col("promo.end_date"))),
                    'left'
                ).select (
                    clean_sale_df.sale_id.alias('sale_id'),
                    clean_sale_df.order_id.alias('order_id'),
                    clean_sale_df.product_id.alias('product_id'),
                    clean_sale_df.customer_id.alias('customer_id'),
                    col('promo.promotion_id').alias('promotion_id'),
                    clean_sale_df.store_id.alias('store_id'),
                    (clean_sale_df.quantity * clean_product_df.price).alias('sales_amount'),
                    clean_sale_df.quantity.alias('quantity'),
                    when(col('promo.discount_percentage').isNotNull(),
                        (clean_product_df.price * clean_sale_df.quantity * col('promo.discount_percentage')/100))
                    .otherwise(0).alias('discount_amount'),
                   date_format(to_date(clean_order_df.order_date), 'yyyy-MM-dd').alias('order_date')
                    

                )


fact_table_df.show(5)
fact_table_df.printSchema()
fact = fact_table_df.coalesce(1)
fact.write.csv(f"gs://{bucket_name1}/fact",header=False)


# dim_customer = clean_customer_df.select(
#     col("customer_id").alias("customer_id"),
#     trim(col("first_name")).alias('first_name'),
#     trim(col("last_name")).alias('last_name'),
#     trim(col('email')).alias('email'),
#     trim(col('state')).alias('state'),
#     trim(col('zipcode')).alias('zipcode')
# )
# dim_customer = dim_customer.coalesce(1)
# dim_customer.printSchema()
# dim_customer.write.csv(f"gs://{bucket_name1}/dim_customer",header=False)


# dim_prodcut_category = clean_category_df.select(
#     col('category_id').alias('category_id'),
#     trim(col('category_name')).alias('category_name')
# )

# dim_prodcut_category = dim_prodcut_category.coalesce(1)
# dim_prodcut_category.printSchema()
# dim_prodcut_category.write.csv(f"gs://{bucket_name1}/dim_prodcut_category",header=False)

# dim_prodcut = clean_product_df.select(
#     col('product_id').alias('product_id'),
#     trim(col('product_name')).alias('product_name'),
#     trim(col('category_id')).alias('category_id'),
#     trim(col('price')).alias('price')
# )

# dim_prodcut = dim_prodcut.coalesce(1)
# dim_prodcut.printSchema()
# dim_prodcut.write.csv(f"gs://{bucket_name1}/dim_prodcut",header=False)

# dim_promotion = promotion_combined_df.select(
#     col('promotion_id').alias('promotion_id'),
#     trim(col('promotion_name')).alias('promotion_name'),
#     to_date(trim(col('start_date')).alias('start_date')),
#     to_date(trim(col('end_date')).alias('end_date'))
# )

# dim_promotion = dim_promotion.coalesce(1)
# dim_promotion.show(5)
# dim_promotion.printSchema()
# dim_promotion.write.csv(f"gs://{bucket_name1}/dim_promotion",header=False)

# dim_store_location = clean_store_df.select(
#     col('store_id').alias('store_id'),
#     trim(col('city')).alias('city'),
#     trim(col('state')).alias('state'),
#     trim(col('zip_code')).alias('zip_code')
# )

# dim_store_location = dim_store_location.coalesce(1)
# dim_store_location.printSchema()
# dim_store_location.write.csv(f"gs://{bucket_name1}/dim_store_location",header=False)



from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def start_spark():
    spark = SparkSession.builder \
        .appName("OlistTransformation") \
        .getOrCreate()
    return spark

spark = start_spark()

def create_fact_sales(spark, data_path="./data"):
    orders_df = spark.read.csv(f"{data_path}/olist_orders_dataset.csv", header=True, inferSchema=True)
    items_df = spark.read.csv(f"{data_path}/olist_order_items_dataset.csv", header=True, inferSchema=True)

    fact_sales = orders_df.join(items_df, "order_id", "inner") \
        .select(
            "order_id", 
            "customer_id", 
            "product_id", 
            col("price").alias("sale_amount"), 
            "order_purchase_timestamp"
        )
    
    return fact_sales

#test
sales_df = create_fact_sales(spark)
sales_df.show(5)

from pyspark.sql.functions import sum as _sum

def calculate_category_revenue(spark, fact_sales_df, data_path="./data"):
    products_df = spark.read.csv(f"{data_path}/olist_products_dataset.csv", header=True, inferSchema=True)
    translation_df = spark.read.csv(f"{data_path}/product_category_name_translation.csv", header=True, inferSchema=True)


    #  join Fact -> Products -> Translation to get English category names
    rich_sales = fact_sales_df.join(products_df, "product_id", "left") \
        .join(translation_df, "product_category_name", "left")

    # aggregate revenue by category
    category_revenue = rich_sales.groupBy("product_category_name_english") \
        .agg(_sum("sale_amount").alias("total_revenue")) \
        .orderBy(col("total_revenue").desc())
    
    return category_revenue

# test
revenue_df = calculate_category_revenue(spark, sales_df)
revenue_df.show(10)

from pyspark.sql.functions import sum as _sum, desc, count, avg

def get_top_spending_customers(spark, fact_sales_df):
    #who are our best customers?
    return fact_sales_df.groupBy("customer_id") \
        .agg(_sum("sale_amount").alias("total_spent")) \
        .orderBy(desc("total_spent"))

def get_shipping_efficiency(spark, data_path="./data"):
    #which states have the highest average freight cost?
    items_df = spark.read.csv(f"{data_path}/olist_order_items_dataset.csv", header=True, inferSchema=True)
    sellers_df = spark.read.csv(f"{data_path}/olist_sellers_dataset.csv", header=True, inferSchema=True)
    
    return items_df.join(sellers_df, "seller_id") \
        .groupBy("seller_state") \
        .agg(avg("freight_value").alias("avg_shipping_cost")) \
        .orderBy(desc("avg_shipping_cost"))

def get_order_volume_by_hour(spark, fact_sales_df):
    #what time of day do people shop most?
    from pyspark.sql.functions import hour
    return fact_sales_df.withColumn("hour", hour("order_purchase_timestamp")) \
        .groupBy("hour") \
        .count() \
        .orderBy(desc("count"))
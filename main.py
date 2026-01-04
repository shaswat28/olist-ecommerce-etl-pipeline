import sys
from extract import extract_data
from transform import start_spark, create_fact_sales, calculate_category_revenue, get_top_spending_customers, get_shipping_efficiency, get_order_volume_by_hour
from load import load_to_mysql

def run_pipeline():
    print("Starting Olist Data Pipeline")
    
    try:
        file_map = extract_data("./data")
        
        spark = start_spark()
        fact_sales = create_fact_sales(spark)
        category_revenue = calculate_category_revenue(spark, fact_sales)
        top_customers = get_top_spending_customers(spark, fact_sales)
        shipping_costs = get_shipping_efficiency(spark, "./data")
        hourly_trends = get_order_volume_by_hour(spark, fact_sales)

        db_password = "" 
        load_to_mysql(fact_sales, "fact_sales", db_password)
        load_to_mysql(category_revenue, "category_revenue_report", db_password)
        load_to_mysql(top_customers, "top_spending_customers", db_password)
        load_to_mysql(shipping_costs, "shipping_efficiency_by_state", db_password)
        load_to_mysql(hourly_trends, "hourly_order_trends", db_password)
        
        print("Pipeline Execution Successful")
        
    except Exception as e:
        print(f"PIPELINE FAILURE: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()

from pyspark.sql import SparkSession
import numpy as np

import prepare_dataset
import utils
import sql_query


def main():
    # Create a SparkSession
    spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

    # Load the dataset into DataFrames
    customer_df = prepare_dataset.load_dataset(spark, "dataset/olist_customers_dataset.csv")
    geolocation_df = prepare_dataset.load_dataset(spark, "dataset/olist_geolocation_dataset.csv")
    orders_df = prepare_dataset.load_dataset(spark, "dataset/olist_orders_dataset.csv")
    orders_df_with_day_month = utils.convert_datetime_to_day_month(orders_df, "order_purchase_timestamp")
    order_items_df = prepare_dataset.load_dataset(spark, "dataset/olist_order_items_dataset.csv")
    products_df = prepare_dataset.load_dataset(spark, "dataset/olist_products_dataset.csv")
    sellers_df = prepare_dataset.load_dataset(spark, "dataset/olist_sellers_dataset.csv")
    product_category_name_translation_df = prepare_dataset.load_dataset(spark, "dataset/product_category_name_translation.csv")


    customer_df.createOrReplaceTempView("customers")
    geolocation_df.createOrReplaceTempView("geolocations")
    orders_df_with_day_month.createOrReplaceTempView("orders")
    order_items_df.createOrReplaceTempView("order_items")
    products_df.createOrReplaceTempView("products")
    sellers_df.createOrReplaceTempView("sellers")
    product_category_name_translation_df.createOrReplaceTempView("product_name_translation")


    # Execute the SQL query
    # RFM customer segmnetation
    customer_frequency = spark.sql(sql_query.get_query("get_customer_frequency"))
    customer_recency= spark.sql(sql_query.get_query("get_customer_recency"))
    customer_monetary = spark.sql(sql_query.get_query("get_customer_monetary"))

    customer_frequency.createOrReplaceTempView("customer_frequency")
    customer_recency.createOrReplaceTempView("customer_recency")
    customer_monetary.createOrReplaceTempView("customer_monetary")
    rfm_df = spark.sql(sql_query.get_query("get_rfm_dataframe"))
    rfm_df.createOrReplaceTempView("rfm_df")

    # Assign scores for recency, frequency, and monetary (spend)
    rfm_df = utils.assign_rfm_scores(rfm_df, "recency", "recency_score", reverse=True)
    rfm_df = utils.assign_rfm_scores(rfm_df, "frequency", "frequency_score")
    rfm_df = utils.assign_rfm_scores(rfm_df, "spend", "monetary_score")

    rfm_score_df = utils.assign_total_rfm_score(rfm_df, "rfm_score_col_header")
    rfm_score_df.createOrReplaceTempView("rfm_scores")

    # Assign clusters based on RFM scores
    rfm_clustered_df = spark.sql(sql_query.get_query("get_rfm_customer_groups"))

    # Extract the clusters and numer of customers in each cluster for plotting
    segmented_customers = np.array([[row['cluster'], row['num_customers']] for row in rfm_clustered_df.collect()])
    labels = ["Top Customers", "High Value Customer", "Medium Customer", "Low Value Customer", "Lost Customer"]
    utils.plot_pie_chart(segmented_customers, labels)
    

    # Execute the SQL query
    # Get the numer of orders for each state and plot a map for visualization
    states_orders = spark.sql(sql_query.get_query("get_oreders_per_state"))
    geo_states = spark.sql(sql_query.get_query("get_states_geo_coordinates"))
    utils.plot_geo_states(states_orders, "customer_state", "num_customers", geo_states, "customers_map.png")


    # Execute the SQL query
    # Get the number of orders for each date and vizualize the results
    datetime_orders = spark.sql(sql_query.get_query("get_datetime_orders"))
    # Extract the dates and number of orders for plotting
    dates_orders = np.array([[row['datetime'], row['num_orders']] for row in datetime_orders.collect()])
    utils.plot_line_bar(dates_orders, "Number of orders per date", "date", "number of orders")
    

    # Execute the SQL query
    # Get the number of orders for each day and month and vizualize the results
    month_orders = spark.sql(sql_query.get_query("get_months_orders"))
    # Extract the month and number of orders for plotting
    month_orders = np.array([[row['month'], row['num_orders']] for row in month_orders.collect()])
    utils.plot_line_bar(month_orders, "Number of orders per month", "month", "number of orders")

    days_orders = spark.sql(sql_query.get_query("get_days_orders"))
    # Extract the day and number of orders for plotting
    day_orders = np.array([[row['day'], row['num_orders']] for row in days_orders.collect()])
    utils.plot_line_bar(day_orders, "Number of orders per day", "day", "number of orders")


    # Execute the SQL query
    # Get 10 best-selling products based on number of orders
    best_selling_products = spark.sql(sql_query.get_query("get_best_selling_products"))
    # Extract the product category and number of orders for plotting
    best_products = np.array([[row['product_category_name'], row['num_orders']] for row in best_selling_products.take(10)])
    utils.plot_bar_chart(best_products, "Best-selling products", "product", "number of orders")


    # Execute the SQL query
    # Get 10 most expensive products based on their prices
    products_prices = spark.sql(sql_query.get_query("get_most_expensive_products"))
    products_prices.createOrReplaceTempView("products_prices")
    products_prices = spark.sql(sql_query.get_query("translate_products_name"))
    # Extract the product category and average prices for plotting
    most_expensive_product_prices = np.array([[row['product_category_name'], row['average_price']] for row in products_prices.take(10)])
    utils.plot_bar_chart(most_expensive_product_prices, "Most expensive products",\
                         "product", "average price", wrapped_labels=True)


    # Execute the SQL query
    # Get top 10 sellers for each year and top 10 sellers for whole given period based on their purchase
    top_sellers = spark.sql(sql_query.get_query("get_top_sellers"))
    # Extract the seller id and number of orders for plotting
    first_top_10_sellers = np.array([[row['seller_id'], row['num_orders']] for row in top_sellers.take(10)])
    utils.plot_bar_chart(first_top_10_sellers, "Best sellers", "seller ID", "number of orders")

    spark.stop()


if __name__ == '__main__':
    main()

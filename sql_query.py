
queries= {
        "get_oreders_per_state" : """
            SELECT customer_state, 
            COUNT(customer_id) AS num_customers,
            COLLECT_LIST(customer_id) AS customer_ids 
            FROM customers
            GROUP BY customer_state
            """
        ,
        
        "get_states_geo_coordinates" : """
            SELECT geolocation_state,
            AVG(geolocation_lat) AS geolocation_lat,
            AVG(geolocation_lng) AS geolocation_lng
            FROM geolocations
            GROUP BY geolocation_state
            """
        ,

        "get_city_geo_coordinates" : """
            SELECT geolocation_city,
            AVG(geolocation_lat) AS geolocation_lat,
            AVG(geolocation_lng) AS geolocation_lng
            FROM geolocations
            GROUP BY geolocation_city
            """
        ,

        "get_datetime_orders" : """
            SELECT CAST(order_purchase_timestamp AS DATE) AS datetime,
            COUNT(*) AS num_orders
            FROM orders
            GROUP BY CAST(order_purchase_timestamp AS DATE)
            ORDER BY CAST(order_purchase_timestamp AS DATE)
        """
        ,

        "get_months_orders" : """
            SELECT month,
            COUNT(*) AS num_orders
            FROM orders
            GROUP BY month
            ORDER BY 
                CASE
                    WHEN month = 'January' THEN 1
                    WHEN month = 'February' THEN 2
                    WHEN month = 'March' THEN 3
                    WHEN month = 'April' THEN 4
                    WHEN month = 'May' THEN 5
                    WHEN month = 'June' THEN 6
                    WHEN month = 'July' THEN 7
                    WHEN month = 'August' THEN 8
                    WHEN month = 'September' THEN 9
                    WHEN month = 'October' THEN 10
                    WHEN month = 'November' THEN 11
                    WHEN month = 'December' THEN 12
                END ASC
        """
        ,
        
        "get_days_orders" : """
            SELECT day,
            COUNT(*) AS num_orders
            FROM orders
            GROUP BY 
                day
            ORDER BY 
                CASE
                    WHEN day = 'Sunday' THEN 1
                    WHEN day = 'Monday' THEN 2
                    WHEN day = 'Tuesday' THEN 3
                    WHEN day = 'Wednesday' THEN 4
                    WHEN day = 'Thursday' THEN 5
                    WHEN day = 'Friday' THEN 6
                    WHEN day = 'Saturday' THEN 7
                END ASC

        """
        ,

        "get_best_selling_products" : """
            SELECT
                p.product_category_name,
                COUNT(o.order_id) AS num_orders
            FROM 
                products p
            INNER JOIN 
                order_items o ON p.product_id = o.product_id
            GROUP BY 
                p.product_category_name
            ORDER BY 
                num_orders DESC
        """
        ,

        "get_most_expensive_products":"""
            SELECT 
                p.product_category_name, 
                AVG(o.price) AS average_price
            FROM 
                products p
            JOIN 
                order_items o ON p.product_id = o.product_id
            GROUP BY 
                p.product_category_name
            ORDER BY 
                average_price DESC

        """
        ,

        "get_top_sellers":"""
            SELECT
                s.seller_id,
                s.seller_zip_code_prefix,
                s.seller_city,
                COUNT(*) AS num_orders
            FROM 
                sellers s
            INNER JOIN 
                order_items o ON s.seller_id = o.seller_id
            GROUP BY 
                s.seller_id, s.seller_city, s.seller_zip_code_prefix
            ORDER BY 
                num_orders DESC
            
        """
        ,

        "translate_products_name":"""
            SELECT 
                pnt.product_category_name_english AS product_category_name,
                pp.average_price
            FROM 
                products_prices pp
            JOIN 
                product_name_translation pnt ON pnt.product_category_name = pp.product_category_name
            ORDER BY
                average_price DESC
        """
        ,

        "get_customer_frequency":"""
            SELECT 
                customer_unique_id, 
                COUNT(*) AS frequency
            FROM 
                orders o
            JOIN
                customers c ON c.customer_id = o.customer_id
            WHERE order_status == "delivered"
            GROUP BY 
                customer_unique_id
            ORDER BY
                frequency DESC
        """
        ,

        "get_customer_recency":"""
            SELECT 
                customer_unique_id, 
                MAX(order_purchase_timestamp) AS last_purchase_time
            FROM 
                orders o
            JOIN
                customers c ON c.customer_id = o.customer_id
            WHERE order_status == "delivered"
            GROUP BY 
                customer_unique_id
            ORDER BY
                last_purchase_time DESC
        """
        ,

        "get_customer_monetary":"""
            SELECT 
                customer_unique_id, 
                SUM(price+freight_value) AS spend
            FROM 
                orders o
            JOIN
                order_items oi ON o.order_id = oi.order_id
            JOIN
                customers c ON c.customer_id = o.customer_id
            WHERE order_status == "delivered"
            GROUP BY 
                customer_unique_id
            ORDER BY
                spend DESC
        """
        ,

        "get_rfm_dataframe" : """
            SELECT 
                cf.customer_unique_id,
                DATEDIFF(day, cr.last_purchase_time, CURRENT_TIMESTAMP) AS recency,
                cm.spend,
                cf.frequency

            FROM 
                customer_frequency cf
            JOIN
                customer_recency cr ON cr.customer_unique_id = cf.customer_unique_id
            JOIN
                customer_monetary cm ON cm.customer_unique_id = cf.customer_unique_id
            ORDER BY
                cf.frequency DESC
        """
        ,
        "get_rfm_customer_groups":"""
            SELECT 
                rfm_final_score AS cluster,
                COUNT(customer_unique_id) AS num_customers 
            FROM 
                rfm_scores
            GROUP BY rfm_final_score
        """
}


def get_query(query_name):
    return queries[query_name]
    
-- How many stores does the business have and in which countries?
SELECT country_code,
    COUNT(*)
FROM dim_store_details
GROUP BY country_code
ORDER BY COUNT(*) DESC;
-- Which locations currently have the most stores?
SELECT locality,
    COUNT(*)
FROM dim_store_details
GROUP BY locality
ORDER BY COUNT(*) DESC;
-- Which months produced the largest amount of sales?
WITH sale AS (
    SELECT orders_table.date_uuid,
        dim_products.product_price * orders_table.product_quantity as single_sale
    FROM orders_table
        JOIN dim_products on dim_products.product_code = orders_table.product_code
)
SELECT ROUND(CAST(SUM(sale.single_sale) AS numeric), 2) AS total_sales,
    dim_date_times.month
FROM sale
    JOIN dim_date_times ON sale.date_uuid = dim_date_times.date_uuid
GROUP BY dim_date_times.month;
-- How many sales are coming from online? 
SELECT COUNT(*),
    SUM(orders_table.product_quantity),
    derived_table.location
FROM orders_table
    JOIN (
        SELECT store_code,
            CASE
                WHEN store_type = 'Web Portal' THEN 'Web'
                ELSE 'Offline'
            END AS location
        FROM dim_store_details
    ) AS derived_table ON orders_table.store_code = derived_table.store_code
GROUP BY derived_table.location;
-- What percentage of sales come through each type of store
WITH single_sales as (
    SELECT dim_products.product_price * orders_table.product_quantity as single_sale,
        store_code
    FROM orders_table
        JOIN dim_products on dim_products.product_code = orders_table.product_code
),
sales_total_per_store as (
    SELECT ROUND(CAST(SUM(single_sale) AS numeric), 2) AS sales_total_per_store,
        dim_store_details.store_type as store_type
    FROM single_sales
        JOIN dim_store_details on single_sales.store_code = dim_store_details.store_code
    GROUP BY store_type
),
total_sales AS (
    SELECT SUM(single_sale) as total_sales
    FROM single_sales
)
SELECT store_type,
    sales_total_per_store.sales_total_per_store AS total_sales,
    ROUND(
        CAST(
            (
                (
                    sales_total_per_store.sales_total_per_store / total_sales.total_sales
                ) * 100
            ) AS numeric
        ),
        2
    ) AS percentage_total
FROM sales_total_per_store,
    total_sales
ORDER BY total_sales DESC;
-- Which month in each year produced the highest cost of sales?
WITH single_sales as (
    SELECT dim_products.product_price * orders_table.product_quantity as single_sale,
        date_uuid
    FROM orders_table
        JOIN dim_products on dim_products.product_code = orders_table.product_code
)
SELECT ROUND(
        CAST(SUM(single_sales.single_sale) as numeric),
        2
    ) as total_sales,
    dim_date_times.year,
    dim_date_times.month
FROM single_sales
    JOIN dim_date_times on dim_date_times.date_uuid = single_sales.date_uuid
GROUP BY dim_date_times.year,
    dim_date_times.month
ORDER BY total_sales DESC
LIMIT 10;
-- What is our staff headcount?
SELECT SUM(staff_numbers) as total_staff_numbers,
    country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;
-- Which German store type is selling the most 
WITH single_sales as (
    SELECT dim_products.product_price * orders_table.product_quantity as single_sale,
        store_code
    FROM orders_table
        JOIN dim_products on dim_products.product_code = orders_table.product_code
)
SELECT ROUND(CAST(SUM(single_sale) AS numeric), 2) AS total_sales,
    dim_store_details.store_type as store_type,
    dim_store_details.country_code as country_code
FROM single_sales
    JOIN dim_store_details on single_sales.store_code = dim_store_details.store_code
GROUP BY store_type,
    country_code
HAVING country_code = 'DE'
ORDER BY total_sales;
-- How quickly is the company making sales? in PROGRESS
ALTER TABLE dim_date_times
ADD COLUMN date_time_stamp VARCHAR(40)
UPDATE dim_date_times
SET date_time_stamp = CONCAT(year, '-', month, '-', day, ' ', timestamp);
ALTER TABLE dim_date_times
ALTER COLUMN date_time_stamp TYPE TIMESTAMP USING date_time_stamp::timestamp without time zone WITH timestamps as (
        SELECT year,
            date_time_stamp,
            LEAD(date_time_stamp, 1) OVER (
                ORDER BY date_time_stamp
            ) date_time_stamp_minus_one
        FROM dim_date_times
    ),
    timestamp_diff as (
        SELECT year,
            date_time_stamp_minus_one - date_time_stamp as time_diff
        FROM timestamps
    ),
    average_time as (
        SELECT year,
            AVG(time_diff) as avg_time_diff
        FROM timestamp_diff
        GROUP BY year
    )
SELECT year,
    json_build_object(
        'hours',
        date_part('hour', avg_time_diff),
        'minutes',
        date_part('minute', avg_time_diff),
        'seconds',
        date_part('second', date_trunc('second', avg_time_diff)),
        'milliseconds',
        SUBSTRING(
            date_part('milliseconds', avg_time_diff)::text,
            3,
            100
        )::FLOAT
    ) AS actual_time_taken
FROM average_time;
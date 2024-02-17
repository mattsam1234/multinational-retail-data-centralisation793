
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(20),
ALTER COLUMN product_code TYPE VARCHAR(20),
ALTER COLUMN product_quantity TYPE SMALLINT;

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN date_of_birth TYPE date,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN join_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(255),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(255),
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(30);

UPDATE dim_products 
SET weight_class = 
            CASE 
                WHEN weight < 2 THEN 'Light'
                WHEN weight BETWEEN 2 AND 40 THEN 'Mid_size'
                WHEN weight BETWEEN 41 AND 140 THEN 'Heavy'
                ELSE 'Truck_required'
            END;
        
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products 
SET still_available = 
            CASE 
                WHEN still_available = 'Still_avaliable' THEN 1
                ELSE '0'
            END;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT,
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(255),
ALTER COLUMN product_code TYPE VARCHAR(255),
ALTER COLUMN uuid TYPE UUID
USING uuid::uuid,
ALTER COLUMN still_available TYPE bool
USING still_available::boolean;



ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);


ALTER TABLE orders_table
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);


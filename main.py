from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd

#initialise engine to connect to local database
local_engine = DatabaseConnector('db_creds_local.yaml')

#initialise engine to connect to aws database
aws_engine = DatabaseConnector('db_creds.yaml')

#get user data from aws
dirty_user_data = DataExtractor().read_rds_table(table_name='legacy_users', db_connector=aws_engine)
#clean the data
user_data_cleaner = DataCleaning(dirty_user_data)
cleaned_data = user_data_cleaner.clean_users()
#send to local
local_engine.upload_to_db(df=cleaned_data, table='dim_users')

#get the card data from pdf
dirty_pdf_data = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#clean the card data
pdf_data_cleaner = DataCleaning(dirty_pdf_data)
cleaned_pdf_data = pdf_data_cleaner.clean_card_data()
#send card data to local
local_engine.upload_to_db(df=cleaned_pdf_data, table='dim_card_details')

#Pull data from API - Commented out so not to spend 4mins pulling each time
#api_pulling_extractor = DataExtractor()
#number_of_stores = api_pulling_extractor.list_number_of_stores()
#headers_dict = (insert header dictionary)
#endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
#column_headers_list = api_pulling_extractor.get_column_headers(endpoint=endpoint, header_dict=headers_dict )
#api_pulling_extractor.retrieve_stores_data_to_csv(number_of_stores=number_of_stores, endpoint=endpoint, header_dict=headers_dict, column_header_list=column_headers_list)


#Data was pulled from api and collated into csv using the retrieve_stores_data_to_csv function in data extractor 
#get store data from csv
dirty_api_data = pd.read_csv('store.csv', index_col='index')
#clean store data
api_data_cleaner = DataCleaning(dirty_api_data)
cleaned_api_data = api_data_cleaner.clean_store_data()
#push store data to local
local_engine.upload_to_db(df=cleaned_api_data, table='dim_store_details')

#Pull order data from s3 bucket and save csv
dirty_product_data = DataExtractor().extract_from_csv('product_data.csv')
#pass the dirty product data into data cleaning instance and then run the function to clean the data
product_data_cleaner = DataCleaning(dirty_product_data)
clean_product_data = product_data_cleaner.clean_products_data()

local_engine.upload_to_db(df=clean_product_data, table='dim_products')

#Get orders table from AWS
dirty_order_data = DataExtractor().read_rds_table(table_name='orders_table', db_connector=aws_engine)
#clean order data
order_data_cleaner = DataCleaning(dirty_order_data)
clean_order_data = order_data_cleaner.clean_order_data()
#push order data to local
local_engine.upload_to_db(df=clean_order_data, table='orders_table')

dirty_events_data = DataExtractor().extract_json_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
event_data_cleaner = DataCleaning(dirty_events_data)
clean_events_data = event_data_cleaner.clean_events_data()

local_engine.upload_to_db(df=clean_events_data, table='dim_date_times')



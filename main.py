from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
from decouple import config

def __init__(self):
    '''
    Initialises the database engine given the credentials 
    Parameters
    ----------
    path_to_credentials : the file path to the credentials
    '''
    #AWS database credentials file name
    aws_creds = config('AWS_CREDS')
    #Local database credentials file name
    local_creds = config('LOCAL_CREDS')
    #initialise engine to connect to local database
    self.local_engine = DatabaseConnector(local_creds)
    # #initialise engine to connect to aws database
    self.aws_engine = DatabaseConnector(aws_creds)

def clean_user_data(self):
    #get user data from aws
    dirty_user_data = DataExtractor().read_rds_table(table_name='legacy_users', db_connector=self.aws_engine)
    #clean the data
    user_data_cleaner = DataCleaning(dirty_user_data)
    cleaned_data = user_data_cleaner.clean_users()
    #send to local
    self.local_engine.upload_to_db(df=cleaned_data, table='dim_users')


def clean_card_data(self):
    #get the card data from pdf
    dirty_pdf_data = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #clean the card data
    pdf_data_cleaner = DataCleaning(dirty_pdf_data)
    cleaned_pdf_data = pdf_data_cleaner.clean_card_data()
    #send card data to local
    self.local_engine.upload_to_db(df=cleaned_pdf_data, table='dim_card_details')
    
def pull_store_data():
    # Pull data from API - Commented out so not to spend 4mins pulling each time
    api_pulling_extractor = DataExtractor()
    number_of_stores = api_pulling_extractor.list_number_of_stores()
    headers_dict = config("HEADER_DICT")
    endpoint = config("ENDPOINT")
    column_headers_list = api_pulling_extractor.get_column_headers(endpoint=endpoint, header_dict=headers_dict )
    api_pulling_extractor.retrieve_stores_data_to_csv(number_of_stores=number_of_stores, endpoint=endpoint, header_dict=headers_dict, column_header_list=column_headers_list)

def clean_store_data(self):
    #get store data from csv
    dirty_api_data = pd.read_csv('store.csv', index_col='index')
    #clean store data
    api_data_cleaner = DataCleaning(dirty_api_data)
    cleaned_api_data = api_data_cleaner.clean_store_data()
    #push store data to local
    self.local_engine.upload_to_db(df=cleaned_api_data, table='dim_store_details')
    
def clean_product_data(self):
    #Pull order data from s3 bucket and save csv
    dirty_product_data = DataExtractor().extract_from_csv('product_data.csv')
    #pass the dirty product data into data cleaning instance and then run the function to clean the data
    product_data_cleaner = DataCleaning(dirty_product_data)
    clean_product_data = product_data_cleaner.clean_products_data()
    self.local_engine.upload_to_db(df=clean_product_data, table='dim_products')

def clean_order_data(self):
    #Get orders table from AWS
    dirty_order_data = DataExtractor().read_rds_table(table_name='orders_table', db_connector=self.aws_engine)
    #clean order data
    order_data_cleaner = DataCleaning(dirty_order_data)
    clean_order_data = order_data_cleaner.clean_order_data()
    #push order data to local
    self.local_engine.upload_to_db(df=clean_order_data, table='orders_table')

def clean_events_data(self):
    dirty_events_data = DataExtractor().extract_json_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    event_data_cleaner = DataCleaning(dirty_events_data)
    clean_events_data = event_data_cleaner.clean_events_data()
    self.local_engine.upload_to_db(df=clean_events_data, table='dim_date_times')

if __name__ == "__main__":    
    clean_user_data()
    clean_card_data()
    pull_store_data()
    clean_store_data()
    clean_product_data()
    clean_order_data()
    clean_events_data()

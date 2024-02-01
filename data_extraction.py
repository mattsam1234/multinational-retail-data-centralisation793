from database_utils import DatabaseConnector
from botocore.exceptions import  ClientError
from pathlib import Path
import boto3
import pandas as pd
import tabula
import requests


class DataExtractor:
    """
    A Class to extract data from various sources
    
    Attributes
    ----------
    None
    
    Methods
    -------
    read_rds_table : Extract data from an Amazon RDS table
    retrieve_pdf_data : Extract data from a PDF
    list_number_of_stores : List the number of stores from the "Retrieve a store" API
    retrieve_stores_data_to_csv : Pull each of the stores data by calling the "Return the number of stores" Api once per store and appending to a csv stored in folder where this function is run
    get_column_headers : Get the column headers from stores api
    extract_from_s3 : Extract product_data.csv file from an s3 bucket. Creates a file of the same name in the directory this function is run in to store the data
    extract_from_csv : Extract data from a CSV file.
    extract_json_from_s3 : Extract JSON data from an AWS S3 bucket.
    
    """
    
    def __init__(self):
        pass
    
    def read_rds_table(self, db_connector : DatabaseConnector, table_name: str):
        '''
        Extract data from an Amazon RDS table
        Parameters
        ----------
        db_connector : a DatabaseConnector object
        table_name(str) : the name of the table to pull from
        
        Returns 
        -------
        df : Pandas Dataframe object
        '''
        df = pd.read_sql_table(table_name, db_connector.engine)
        return df
    
    def retrieve_pdf_data(self, path_to_pdf: str):
        '''
        Extract data from a PDF
        Parameters
        ----------
        path_to_pdf(str) : Path or URL of the PDF
        
        Returns 
        -------
        df : Pandas Dataframe object
        '''
        tables = tabula.read_pdf(path_to_pdf, pages='all')
        df = pd.concat(tables)
        return df
    
    def list_number_of_stores(self, endpoint:str, header_dict:dict[str, str]):
        '''
        List the number of stores from the "Retrieve a store" API
        
        Parameters
        ----------
        endpoint(str) : Api endpoint
        header_dict(dict[str, str]) : The dictionary of headers to be send to the API
        
        Returns 
        -------
        stores_data : The number of stores
        '''
        response = requests.get(endpoint, headers=header_dict)
        if response.status_code == 200:
            data = response.json()
            stores_data = data['number_stores']
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
        return stores_data
    
    def retrieve_stores_data_to_csv(self, number_of_stores: int, endpoint:str, header_dict:dict[str, str], column_header_list:list[str]):
        '''
        Pull each of the stores data by calling the "Return the number of stores" Api once per store and appending to a csv stored in folder where this function is run
        
        Parameters
        ----------
        number_of_stores(int) : The number of stores to pull data from
        endpoint(str) : Api endpoint
        header_dict(dict[str, str]) : The dictionary of headers to be send to the API
        
        Returns 
        -------
        none
        '''
        for i in range(0, number_of_stores):
            response = requests.get(f'{endpoint}{i}', headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                df_temp = pd.DataFrame([data])
                
                if i > 0:
                    mode = 'a'
                    header = 0
                else:
                    mode = 'w'
                    header = column_header_list
                df_temp.to_csv('store.csv', mode=mode, header=header)
                
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        
    def get_column_headers(self, endpoint:str, header_dict:dict[str, str]):
        '''
        Get the column headers from stores api        
        
        Parameters
        ----------
        endpoint(str) : Api endpoint
        header_dict(dict[str, str]) : The dictionary of headers to be send to the API
        
        Returns 
        -------
        column_headers : List of column headers
        '''
        response = requests.get(f'{endpoint}0', headers=header_dict)
        column_headers = []
        if response.status_code == 200:
                data = response.json()
                for key in data.keys():
                    column_headers.append(key)
        return column_headers
    
    def extract_from_s3(self, address:str):
        '''
        Extract product_data.csv file from an s3 bucket. Creates a file of the same name in the directory this function is run in to store the data
        
        Parameters
        ----------
        address(str): The S3 bucket address
        
        Returns 
        -------
        None
        '''
        components = address.split('/')
        bucket_name = components[2]
        file_path = components[3]
        Path("/product_data.csv").mkdir(parents=True, exist_ok=True)
        s3 = boto3.client('s3')
        try:
            s3.download_file(bucket_name, file_path, 'product_data.csv')
        except ClientError as e: 
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
            
    def extract_from_csv(self, path_to_csv:str):
        '''
        Extract data from a CSV file.
        
        Parameters
        ----------
        path_to_csv(str) : The path to the CSV file
        
        Returns 
        -------
        df : Pandas dataframe of the data
        '''
        df = pd.read_csv(path_to_csv, index_col= [0])
        return df
    
    def extract_json_from_s3(self, address:str):
        '''
        Extract JSON data from an AWS S3 bucket.
        
        Parameters
        ----------
        address(str) : The S3 object URL of the JSON file 
        
        Returns 
        -------
        df : Pandas dataframe of the data
        '''
        df = pd.read_json(address)
        return df
        

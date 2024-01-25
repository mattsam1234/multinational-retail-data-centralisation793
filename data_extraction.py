from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from botocore.exceptions import  ClientError
from pathlib import Path

import pandas as pd
class DataExtractor:
    
    def __init__(self):
        pass
    
    def read_rds_table(self, db_connector : DatabaseConnector, table_name: str):
        df = pd.read_sql_table(table_name, db_connector.engine)
        return df
    
    def retrieve_pdf_data(self, path_to_pdf: str):
        tables = tabula.read_pdf(path_to_pdf, pages='all')
        df = pd.concat(tables)
        return df
    
    def list_number_of_stores(self, endpoint:str, header_dict:dict[str, str]):
        response = requests.get(endpoint, headers=header_dict)
        if response.status_code == 200:
            data = response.json()
            stores_data = data['number_stores']
        else:
            print("You done fucked up")
        return stores_data
    
    def retrieve_stores_data(self, number_of_stores: int, endpoint:str, header_dict:dict[str, str], column_headers_list:list[str]):
        df = pd.DataFrame()
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
                    header = column_headers_list
                df_temp.to_csv('store.csv', mode=mode, header=header)
                
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        return df
        
    def get_headers(self, endpoint:str, header_dict:dict[str, str]):
        response = requests.get(f'{endpoint}0', headers=header_dict)
        headers = []
        if response.status_code == 200:
                data = response.json()
                for key in data.keys():
                    headers.append(key)
        return headers
    
    def extract_from_s3(self, address:str):
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
        df = pd.read_csv(path_to_csv, index_col= [0])
        return df
    
    def extract_json_from_s3(self, address:str):
        df = pd.read_json(address)
        return df
        

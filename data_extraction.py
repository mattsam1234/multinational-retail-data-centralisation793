from database_utils import DatabaseConnector
import pandas as pd
class DataExtractor:
    
    def __init__(self):
        pass
    
    
    def read_rds_table(self, db_connector : DatabaseConnector, table_name: str):
        df = pd.read_sql_table(table_name, db_connector.engine)
        df.head()
        return df
        
if __name__ == "__main__":
    print("Hello, World!")
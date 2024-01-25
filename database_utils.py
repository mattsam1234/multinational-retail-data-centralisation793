import yaml
from sqlalchemy import create_engine, inspect
class DatabaseConnector:
    def __init__(self, path_to_credentials:str):
        self.engine = self.init_db_engine(path_to_credentials=path_to_credentials)
        self.tables = {}
    
    def read_db_creds(self, path_to_credentials:str):
        with open(path_to_credentials, 'r') as db_creds:
            loaded_creds = yaml.safe_load(db_creds)
        return loaded_creds
    
    def init_db_engine(self, path_to_credentials:str):
        credentials = self.read_db_creds(path_to_credentials)
        host=credentials['RDS_HOST']
        port=credentials['RDS_PORT']
        database=credentials['RDS_DATABASE']
        user=credentials['RDS_USER']
        password=credentials['RDS_PASSWORD']
        
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        print(f'{path_to_credentials} engine initialised')
        
        return engine
        
    def list_db_tables(self):
        inspection = inspect(self.engine)
        schemas = inspection.get_schema_names()

        for schema in schemas:
            print(f"schema: {schema}"  )
            tables = inspection.get_table_names(schema=schema)
            print(f"tables: {tables}")
            
            self.tables[schema] = tables
            
        return self.tables
        
    def upload_to_db(self, df, table:str):
        connection = self.engine.connect()
        df.to_sql(table, connection, if_exists='replace')
        print('data pushed')
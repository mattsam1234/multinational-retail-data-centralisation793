import yaml
from sqlalchemy import create_engine, inspect
class DatabaseConnector:
    def __init__(self, path_to_credentials: str):
        self.path_to_credentials = path_to_credentials
        self.creds = self.read_db_creds
        self.engine = self.init_db_engine
        self.tables = {}
    
    def read_db_creds(self):
        with open(self.path_to_credentials, 'r') as db_creds:
            loaded_creds = yaml.safe_load(db_creds)
        return loaded_creds
    
    def init_db_engine(self):
        
        host=self.creds['RDS_HOST']
        port=self.creds['RDS_PORT']
        database=self.creds['RDS_DATABASE']
        user=self.creds['RDS_USER']
        password=self.creds['RDS_PASSWORD']
        
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        
        return engine
        
    def list_db_tables(self):
        inspection = inspect(self.engine)
        schemas = inspection.get_schema_names()

        for schema in schemas:
            print("schema: %s" % schema)
            tables = inspection.get_table_names(schema=schema)
            
            self.tables[schema] = tables
            
        return self.tables
        
        
if __name__ == "__main__":
    self_conn = DatabaseConnector('db_creds.yaml')
    self_conn.list_db_tables()
        

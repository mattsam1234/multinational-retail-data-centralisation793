import yaml
from sqlalchemy import create_engine, inspect
class DatabaseConnector:
    """
    A class to connect to various databases

    ...

    Attributes
    ----------
    self.engine : stores the SQLalchemy engine based on the credentials given
    self.tables : Empty dictionary populated in the list_db_tables method
    
    Methods
    -------
    list_db_tables : List the schemas and the tables in those schemas
    upload_to_db : Uploads the dataframe to the given table for the engine previously initialised. Will replace any existing table with given name.
    
    """
    def __init__(self, path_to_credentials:str):
        '''
        Initialises the database engine given the credentials 
        Parameters
        ----------
        path_to_credentials : the file path to the credentials
        '''
        self.engine = self._init_db_engine(path_to_credentials=path_to_credentials)
        self.tables = {}
    
    def _read_db_creds(self, path_to_credentials:str):
        '''
        Use Yaml to load a credentials file.
        Parameters
        ----------
        path_to_credentials(str) : Path to the credentials file
        
        Returns 
        -------
        Loaded Credentials
        '''
        with open(path_to_credentials, 'r') as db_creds:
            loaded_creds = yaml.safe_load(db_creds)
        return loaded_creds
    
    def _init_db_engine(self, path_to_credentials:str):
        '''
        Extract the credentials and initialise a SQLalchemy engine object
        Parameters
        ----------
        path_to_credentials(str) : Path to the credentials file
        
        Returns 
        -------
        SQLalchemy engine 
        '''
        credentials = self._read_db_creds(path_to_credentials)
        host=credentials['RDS_HOST']
        port=credentials['RDS_PORT']
        database=credentials['RDS_DATABASE']
        user=credentials['RDS_USER']
        password=credentials['RDS_PASSWORD']
        
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        print(f'{path_to_credentials} engine initialised')
        
        return engine
        
    def list_db_tables(self):
        '''
        List the schemas and the tables in those schemas
        
        Parameters
        ----------
        none
        
        Returns 
        -------
        self.tables : Populates the tables dictionary
        
        '''
        inspection = inspect(self.engine)
        schemas = inspection.get_schema_names()

        for schema in schemas:
            print(f"schema: {schema}"  )
            tables = inspection.get_table_names(schema=schema)
            print(f"tables: {tables}")
            
            self.tables[schema] = tables
            
        return self.tables
        
    def upload_to_db(self, df, table:str):
        '''
        Uploads the dataframe to the given table for the engine previously initialised.
        Will replace any existing table with given name.
        
        Parameters
        ----------
        df : Pandas Dataframe object to be uploaded
        table(str) : Table name to upload the data into 
        
        Returns 
        -------
        SQLalchemy engine 
        '''
        connection = self.engine.connect()
        df.to_sql(table, connection, if_exists='replace')
        print('data pushed')
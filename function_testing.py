import yaml
def read_db_creds():
    with open('db_creds.yaml', 'r') as db_creds:
        loaded_creds = yaml.safe_load(db_creds)
    print(loaded_creds)
        
read_db_creds()

import pyodbc            #connection module
import pandas as pd      #library for data frame
import os
import yaml
from datetime import datetime
# path for config and info file.

file_path = 'C:\\elt\\config.yaml'
file_path_2 = 'C:\\elt\\info.yaml'

def read_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def read_info(file_path_2):
    with open(file_path_2, 'r') as file:
        info = yaml.safe_load(file)
    return info

# fetching information from config file.  
def get_sql_connection(config):
    username = config['source']['username']
    password = config['source']['password']
    server = config['source']['server']
    database = config['source']['database']

    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
    
    return pyodbc.connect(connection_string)

def write_data_in_batches(df, batch_size, output_directory):
    os.makedirs(output_directory, exist_ok=True)

    num_size = len(df) // batch_size + 1

    for i in range(num_size):
        start_index = i * batch_size
        end_index = (i + 1) * batch_size
        batch_data = df.iloc[start_index:end_index]

        output_file_path = f"{output_directory}/{table}_{i + 1}.csv"
        
        batch_data.to_csv(output_file_path, index=False)

if __name__ == "__main__":
   
    config = read_config(file_path)               # Read parameters from the configuration file

    info = read_info(file_path_2)


    connection = get_sql_connection(config)        # Access database credentials

    for table in info['extraction']['tables']:
        print(table)
        sql_query = f'SELECT * FROM {table}'
        df = pd.read_sql_query(sql_query, connection)
        df['inserted_at']=datetime.now()

        
        batch_size = info['extraction']['batch_size']               # Get batch size and target location from the configuration file
        output_directory = info['output']['target_location']

        
        write_data_in_batches(df, batch_size, f"{output_directory}/{table}")      # Write data to the local file system in batches

    
    connection.close()                      # Close the database connection




























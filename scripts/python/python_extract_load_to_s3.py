
import pyodbc
import boto3
import pandas as pd
import os
import yaml
from datetime import datetime
from natsort import natsorted

class SQLServerData:
    def __init__(self, config_path):
        self.config = self.read_config(config_path)
        self.connection = self.get_sql_connection()

    def read_config(self, file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

    def get_sql_connection(self):
        username = self.config['source']['username']
        password = self.config['source']['password']
        server = self.config['source']['server']
        database = self.config['source']['database']

        connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
        return pyodbc.connect(connection_string)

    def write_data_in_batches(self, df, batch_size, output_directory, table):
        os.makedirs(output_directory, exist_ok=True)

        num_batches = len(df) // batch_size + 1

        for i in range(num_batches):
            start_index = i * batch_size
            end_index = (i + 1) * batch_size
            batch_data = df.iloc[start_index:end_index]
            output_file_path = f"{output_directory}/{table}_{i + 1:02d}.csv"
            batch_data.to_csv(os.path.join(output_file_path), index=False)
        
            

    def fetch_data(self, table):
        sql_query = f'SELECT top 2000* FROM {table}'
        df = pd.read_sql_query(sql_query, self.connection)
        df['inserted_at'] = datetime.now()
        
        batch_size = self.config['extraction']['batch_size']
        output_directory = self.config['output']['target_location']

        self.write_data_in_batches(df, batch_size, f"{output_directory}/{table}", table)
        return df

    def close_connection(self):
        self.connection.close()



class S3Uploader:
    def load_credentials(self, aws_credentials_path):
        with open(aws_credentials_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def generate_filename(self, file_path, table_name, file_format):
        creation_timestamp = os.path.getctime(file_path)
        arrival_details = datetime.fromtimestamp(creation_timestamp).strftime("%Y-%m-%d,%H:%M:%S")
        filename = f"{arrival_details}_{table_name}.{file_format.lower()}"
        return filename

    def upload_csv_files_to_s3(self, table_directory,table, bucket_name, aws_access_key_id, aws_secret_access_key):
        local_folder = table_directory
        s3_bucket_name = bucket_name
        s3_folder = table

        s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        for filename in os.listdir(local_folder):
            local_file_path = os.path.join(local_folder, filename)
            s3_key =f"{s3_folder}/{filename}"
            # s3_key = os.path.join(s3_folder, filename)
            
            s3_client.upload_file(local_file_path, s3_bucket_name, s3_key)
            print(f'Uploaded {filename} to {s3_bucket_name}/{s3_key}')
                
def main():
    # Paths to configuration and information files
    config_path = 'C:\\Users\\durge\\GIT project\\de-batch-project\\scripts\\python\\config\\keydata\\config.yaml'
    aws_credentials_path = 'C:\\Users\\durge\\GIT project\\de-batch-project\\scripts\\python\\config\\keydata\\key.yaml'
    
    # S3 Bucket details and local directory
    bucket_name = 'eltproject'
    local_root_directory = 'C:/Users/durge/GIT project/de-batch-project/new_output_batch'

    # Load credentials from the YAML file
    credentials = S3Uploader().load_credentials(aws_credentials_path)
    
    # Extract AWS credentials from the loaded credentials
    aws_access_key_id = credentials['keys']['access_key_id']
    aws_secret_access_key = credentials['keys']['secret_access_key']

    # Create instances of the classes
    sql_fetcher = SQLServerData(config_path)
    s3_uploader = S3Uploader()

    for table in sql_fetcher.config['extraction']['tables']:
        print(f"Fetching data for table: {table}")
        sql_fetcher.fetch_data(table)
        print(f"Uploading data for table: {table}")
        # Update the following line to include the specific table directory
        table_directory = os.path.join(local_root_directory, table)
        print(table_directory)
        s3_uploader.upload_csv_files_to_s3(table_directory,table, bucket_name, aws_access_key_id, aws_secret_access_key)
            
    # Close the SQL Server connection
    sql_fetcher.close_connection()

if __name__ == "__main__":
    main()




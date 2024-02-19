
import pyodbc
import boto3
import pandas as pd
import os
import yaml
from datetime import datetime
from dotenv import load_dotenv

## Load environment variables from .env file which is stored in in the root directory of  project.

load_dotenv()                                  

class SQLServerData:
    def __init__(self):
        self.connection = self.get_sql_connection()

    def get_sql_connection(self):
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')

        connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
        return pyodbc.connect(connection_string)

    def fetch_data(self, table):
        sql_query = f'SELECT * FROM {table}'
        df = pd.read_sql_query(sql_query, self.connection)
        df['inserted_at'] = datetime.now()
        return df

    def close_connection(self):
        self.connection.close()

class S3Uploader:
    def __init__(self, config_path):
        self.s3_config = self.read_config(config_path)

    def read_config(self, config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def load_credentials(self):
        return os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY')

    def delete_old_files(self, bucket_name, prefix):
        aws_access_key_id, aws_secret_access_key = self.load_credentials()

        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # List all objects in the specified bucket with the given prefix
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

        # Delete each object
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Deleted old file: {obj['Key']}")

    def upload_dataframe_to_s3(self, df, table, bucket_name, batch_size):
        s3_folder = table
        aws_access_key_id, aws_secret_access_key = self.load_credentials()

        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        num_batches = len(df) // batch_size + 1

        for i in range(num_batches):
            start_index = i * batch_size
            end_index = (i + 1) * batch_size
            batch_data = df.iloc[start_index:end_index]
            csv_data = batch_data.to_csv(index=False).encode('utf-8')
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            s3_key = f"{s3_folder}/{table}_{int(i) + 1:02d}_{timestamp}.csv"
            s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=s3_key)
            print(f'Uploaded data for {table} to {bucket_name}/{s3_key}')

def main():
    # Create instances of the classes
    sql_fetcher = SQLServerData()
    config_path = os.getenv('CONFIG_PATH')
    s3_uploader = S3Uploader(config_path)

    bucket_name = os.getenv('BUCKET_NAME')

    batch_size = s3_uploader.s3_config['extraction']['batch_size']

    for table in s3_uploader.s3_config['extraction']['tables']:
        print(f"Fetching data for table: {table}")
        df = sql_fetcher.fetch_data(table)
        print(f"Uploading data for table: {table}")
        s3_uploader.delete_old_files(bucket_name, f"{table}")
        s3_uploader.upload_dataframe_to_s3(df, table, bucket_name, batch_size)

    # Close the SQL Server connection
    sql_fetcher.close_connection()

if __name__ == "__main__":
    main()


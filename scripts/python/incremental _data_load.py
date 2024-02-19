import pandas as pd
import os
import yaml
import boto3
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

## Load environment variables from .env file which is stored in in the root directory of  project.

load_dotenv()

class DataFetcher:
    def __init__(self, config_path):
        self.engine = self.create_sqlalchemy_engine()
        self.config = self.read_config(config_path)

    def read_config(self, config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def create_sqlalchemy_engine(self):
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')

        # Construct the connection string with username and password
        connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server'
        return create_engine(connection_string)
    
    def get_latest_runtime_for_table(self, table_name):
        try:
            # Create a DataFrame from CSV file
            df = pd.read_csv("audit.csv")
            # Convert 'Run Timestamp' to datetime format for sorting
            df['Run Timestamp'] = pd.to_datetime(df['Run Timestamp'])
            # Filter DataFrame for the specified table name
            table_df = df[df['Table Name'] == table_name]
            if table_df.empty:
                print(f"No data found for table: {table_name}")
                return pd.to_datetime('2024-01-01 00:00:00.000')  # Set a default date if no data exists
            # Find the latest runtime for the specified table
            latest_runtime = table_df['Run Timestamp'].max()
            # Return the latest runtime for the specified table
            print("Last Run for ",table_name,":",latest_runtime)
            return latest_runtime
        except FileNotFoundError:
            print("File 'audit.csv' not found. Setting default date.")
            return pd.to_datetime('2024-01-01 00:00:00.000')  # Set a default date if the file does not exist


    def save_audit_to_csv(self, audit_data_df, filename="audit.csv"):
        mode = 'a' if os.path.exists(filename) else 'w'
        audit_data_df.to_csv(filename, mode=mode, header=not os.path.exists(filename), index=False)
        print(f"Audit information appended to {filename}")

    def fetch_data_fullload(self, query):
        try:
            df = pd.read_sql_query(query, self.engine)    
            return df
        except Exception as e:
            print(f"Error executing SQL query for full load: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of an error

    def fetch_data_incrementalload(self, query, last_run_timestamp, current_timestamp):
        last_run_timestamp=last_run_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        try:
            full_query = f"{query} WHERE T.TRAN_BEGIN_TIME BETWEEN '{last_run_timestamp}' AND '{current_timestamp}' AND C.__$OPERATION in (1,2,4)"
            df = pd.read_sql_query(full_query, self.engine)
            return df
        except Exception as e:
            print(f"Error executing SQL query for incremental load: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of an error




class S3Uploader:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = self.create_s3_client()

    def load_credentials(self):
        return os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY')

    def create_s3_client(self):
        aws_access_key_id, aws_secret_access_key = self.load_credentials()
        return boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def delete_objects_in_folder(self, s3_folder):
        objects = self.s3_client.list_objects(Bucket=self.bucket_name, Prefix=s3_folder).get('Contents', [])
        for obj in objects:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])

    def upload_dataframe_to_s3(self, df, load_type, table_name, batch_size):
        s3_folder = table_name

        if load_type == 'full':
            self.delete_objects_in_folder(s3_folder)

        # Check if the DataFrame is empty
        if not df.empty:
            if len(df) % batch_size==0:
                num_batches = len(df) // batch_size
            else:
                num_batches = len(df) // batch_size + 1
                   
            for i in range(num_batches):
                start_index = i * batch_size
                end_index = (i + 1) * batch_size
                batch_data = df.iloc[start_index:end_index]
                csv_data = batch_data.to_csv(index=False).encode('utf-8')
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                s3_key = f"{s3_folder}/{table_name}_{timestamp}.csv"
                self.s3_client.put_object(Body=csv_data, Bucket=self.bucket_name, Key=s3_key)
                print(f'Uploaded data for {table_name} to {self.bucket_name}/{s3_key}')
        else:
            print(f'DataFrame for {table_name} is empty. Skipping upload.')

def main():
    config_path = os.getenv('CONFIG_PATH')
    bucket_name = os.getenv('BUCKET_NAME')

    sql_fetcher = DataFetcher(config_path)
    s3_uploader = S3Uploader(bucket_name)

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    audit_data_df = pd.DataFrame(columns=['Run Timestamp', 'Table Name', 'Load Type', 'Rows Extracted'])

    for table_info in sql_fetcher.config['extraction']['tables']:
        table_name = table_info['name']
        load_type = table_info['load_type']
        query = table_info['query']
        batch_size = table_info['batch_size']

        # Use the get_latest_runtime_for_table function
        latest_runtime = sql_fetcher.get_latest_runtime_for_table(table_name)


        if load_type == 'full':
            extracted_df = sql_fetcher.fetch_data_fullload(query)
            rows_extracted = len(extracted_df) if extracted_df is not None else 0
            s3_uploader.upload_dataframe_to_s3(extracted_df, load_type, table_name, batch_size)
        else:
            extracted_df = sql_fetcher.fetch_data_incrementalload(query, latest_runtime, current_timestamp)
            rows_extracted = len(extracted_df) if extracted_df is not None else 0
            s3_uploader.upload_dataframe_to_s3(extracted_df, load_type, table_name, batch_size)

        # Check if the DataFrame is empty before updating the CSV
        if not extracted_df.empty:
            # Add data to the existing DataFrame for audit information
            audit_data_df.loc[len(audit_data_df)] = {
                'Run Timestamp': current_timestamp,
                'Table Name': table_name,
                'Load Type': load_type,
                'Rows Extracted': rows_extracted
            }
            audit_data_df = audit_data_df.drop_duplicates()
            audit_data_df_1 = audit_data_df
           
            
        else:
            print("No Data Found")

    print(extracted_df)

    # Save audit data to CSV locally
    sql_fetcher.save_audit_to_csv(audit_data_df_1)
    

if __name__ == "__main__":
    main()

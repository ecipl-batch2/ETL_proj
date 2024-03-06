import pandas as pd
import os
import yaml
import boto3
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

class DataFetcher:
    def __init__(self, config_path):
        self.sql_engine = self.create_sqlalchemy_engine()
        self.snowflake_conn = self.connect_to_snowflake()
        self.config = self.read_config(config_path)

    def read_config(self, config_path):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def create_sqlalchemy_engine(self):
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')
        connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server'
        return create_engine(connection_string)
    
    def connect_to_snowflake(self):
        snowflake_user = os.getenv('SNOWFLAKE_USER')
        snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
        snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
        snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
        snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
        snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')

        snowflake_params = {
            'account': snowflake_account,
            'user': snowflake_user,
            'password': snowflake_password,
            'warehouse': snowflake_warehouse,
            'database': snowflake_database,
            'schema': snowflake_schema
        }

        # Snowflake connection
        conn = snowflake.connector.connect(**snowflake_params)
        return conn

    def connect_to_snowflake_and_insert(self, audit_data_df):
        conn = self.snowflake_conn
        cursor = conn.cursor()

        try:
            # Iterate over DataFrame rows and insert into the audit_table
            for index, row in audit_data_df.iterrows():
                cursor.execute("INSERT INTO audit_table (run_timestamp, table_name, load_type, rows_extracted) VALUES (%s, %s, %s, %s)",
                            (row['Run Timestamp'], row['Table Name'], row['Load Type'], row['Rows Extracted']))

            # Commit the changes
            conn.commit()

            print("Records inserted successfully.")
        except Exception as e:
            print(f"Error inserting records into audit_table: {e}")
        finally:
            # Close the cursor and connection
            cursor.close()
            conn.close()

    def get_latest_runtime_for_table(self, table_name):
        conn = self.snowflake_conn
        cursor = conn.cursor()

        try:
            # Execute SQL query to retrieve the latest runtime for the specified table
            cursor.execute(f"SELECT MAX(RUN_TIMESTAMP) FROM audit_table WHERE table_name = '{table_name}'")
            latest_runtime = cursor.fetchone()[0]

            if latest_runtime is None:
                print(f"No data found for table: {table_name}")
                return pd.to_datetime('2024-01-01 00:00:00.000')  # Set a default date if no data exists
            else:
                latest_runtime = pd.to_datetime(latest_runtime)
                print("Last Run for ", table_name, ":", latest_runtime)
                return latest_runtime

        except Exception as e:
            print(f"Error retrieving latest runtime for table {table_name} from Snowflake: {e}")
            return pd.to_datetime('2024-01-01 00:00:00.000')  # Set a default date in case of an error
        finally:
            # Close cursor
            cursor.close()


    def fetch_data_fullload(self, query):
        try:
            df = pd.read_sql_query(query, self.sql_engine)    
            return df
        except Exception as e:
            print(f"Error executing SQL query for full load: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of an error

    def fetch_data_incrementalload(self, query, last_run_timestamp, current_timestamp):
        try:
            # Convert timestamps to datetime objects if they are not already
            if not isinstance(last_run_timestamp, datetime):
                last_run_timestamp = pd.to_datetime(last_run_timestamp)
            if not isinstance(current_timestamp, datetime):
                current_timestamp = pd.to_datetime(current_timestamp)
            
            # Format timestamps for the SQL query
            last_run_timestamp = last_run_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            current_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Construct the full SQL query for incremental load
            full_query = f"{query} WHERE T.TRAN_BEGIN_TIME BETWEEN '{last_run_timestamp}' AND '{current_timestamp}' AND C.__$OPERATION in (1,2,4)"

            # Fetch data using the constructed query
            df = pd.read_sql_query(full_query, self.sql_engine)
            return df
        except Exception as e:
            print(f"Error executing SQL query for incremental load: {e}")
            return pd.DataFrame()


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

        rows_extracted = 0  # Initialize rows_extracted with a default value

        if load_type == 'full':
            extracted_df = sql_fetcher.fetch_data_fullload(query)
            if not extracted_df.empty:
                rows_extracted = len(extracted_df)
                s3_uploader.upload_dataframe_to_s3(extracted_df, load_type, table_name, batch_size)
            else:
                print(f'DataFrame for {table_name} is empty. Skipping upload.')
        else:
            extracted_df = sql_fetcher.fetch_data_incrementalload(query, latest_runtime, current_timestamp)
            if not extracted_df.empty:
                rows_extracted = len(extracted_df)
                s3_uploader.upload_dataframe_to_s3(extracted_df, load_type, table_name, batch_size)
            else:
                print(f'DataFrame for {table_name} is empty. Skipping upload.')

        # Add script run data to the audit DataFrame
        audit_data_df.loc[len(audit_data_df)] = {
            'Run Timestamp': current_timestamp,
            'Table Name': table_name,
            'Load Type': load_type,
            'Rows Extracted': rows_extracted
        }


    # Insert script run data into the Snowflake audit_table
    sql_fetcher.connect_to_snowflake_and_insert(audit_data_df)


if __name__ == "__main__":
    main()

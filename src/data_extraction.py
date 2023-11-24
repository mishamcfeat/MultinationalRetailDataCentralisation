import pandas as pd
import tabula
import requests
import logging
import boto3
from typing import Dict, Any
from database_utils import DatabaseConnector


class DataExtractor:
    """
    DataExtractor class for extracting data from various sources including databases, PDFs, and APIs.
    """

    HEADER = {"x-api-key": "[REDACTED]"}
    NUMBER_OF_STORES_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    STORE_DETAILS_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    S3_PATH = "s3://data-handling-public/products.csv"

    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """
        Reads a table from a relational database system (RDS).

        Args:
            db_connector (DatabaseConnector): An instance of DatabaseConnector for database connection.
            table_name (str): The name of the table to be read.

        Returns:
            DataFrame: A DataFrame containing the data from the specified table.

        Raises:
            TypeError: If db_connector is not an instance of DatabaseConnector.
            ValueError: If the specified table is not found in the database.
        """
        # Initialize the database engine and check for table existence
        engine = db_connector.init_db_engine()
        available_tables = db_connector.list_db_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' not found in the database")

        # Execute SQL query to fetch data from the specified table
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)
        return df

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        """
        Extracts data from a PDF file available at the specified URL.

        Args:
            link (str): URL of the PDF file.

        Returns:
            DataFrame: A DataFrame combining all the tables extracted from the PDF.
        """
        # Extract and combine tables from the PDF into a single DataFrame
        dfs = tabula.read_pdf(
            link, pages="all", multiple_tables=True, lattice=True, stream=True
        )
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df

    def list_number_of_stores(self, url=NUMBER_OF_STORES_URL, headers=HEADER) -> dict:
        """
        Retrieves the number of stores from the API.
        """
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    def retrieve_stores_data(self, number_of_stores=451, base_url=STORE_DETAILS_URL, headers=HEADER) -> pd.DataFrame:
        """
        Retrieves details for each store from the API.
        """
        all_stores = []
        for store_number in range(0, number_of_stores):
            try:
                url = f"{base_url}/{store_number}"
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                all_stores.append(response.json())
            except requests.HTTPError as e:
                logging.error(f"Error retrieving data for store {store_number}: {e}")
        return pd.DataFrame(all_stores)

    def extract_from_s3(self, s3_url: str) -> pd.DataFrame:
        """
        Extracts data from an S3 bucket given an HTTP S3 URL. Determines the file
        type (CSV or JSON) based on the URL and reads the data accordingly.

        Args:
            s3_url (str): HTTP URL to the S3 file.

        Returns:
            DataFrame: Data extracted from the S3 file.
        """
        # Parse bucket name and file path from s3_url
        bucket_name = s3_url.split('/')[2].split('.')[0]
        file_path = '/'.join(s3_url.split('/')[3:])

        # Determine file type from the URL
        file_type = file_path.split('.')[-1]

        # Initialise boto3 client
        s3_client = boto3.client("s3")

        # Download file and read into a DataFrame based on file type
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        if file_type.lower() == 'json':
            df = pd.read_json(obj["Body"])
        elif file_type.lower() == 'csv':
            df = pd.read_csv(obj["Body"])
        else:
            raise ValueError("Unsupported file type. Please provide a CSV or JSON file.")

        return df


if __name__ == "__main__":
    extractor = DataExtractor()
    # df = extractor.read_rds_table(db_connector=DatabaseConnector('../config/db_creds.yaml'), table_name='legacy_users')
    # df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    # number_of_stores = extractor.list_number_of_stores()
    # print(number_of_stores)

    # # Fetching store data and saving it to a CSV file
    # store_data = extractor.retrieve_stores_data(number_of_stores["number_stores"])
    # store_data.to_csv("store_data.csv", index=False) 

    # data = extractor.extract_from_s3()

    # df = extractor.read_rds_table(db_connector=DatabaseConnector('../config/db_creds.yaml'), table_name='orders_table')
    # print(df.head())

    # data_json = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    # print(data_json)
    # Constants
    HEADER = {"x-api-key": "[REDACTED]"}
    NUMBER_OF_STORES_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    STORE_DETAILS_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    
    fetcher = DataExtractor()

    # Get the number of stores from the API
    stores_info = fetcher.list_number_of_stores()
    number_of_stores = stores_info.get('number_of_stores', 451)  # Default to 451 if not specified

    # Retrieve store data
    store_data = fetcher.retrieve_stores_data(number_of_stores=number_of_stores)

    # Save to CSV file
    store_data.to_csv('store_data_two.csv', index=False)
    print("Store data saved to store_data_two.csv")
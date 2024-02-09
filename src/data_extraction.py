from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import logging
import boto3


class DataExtractor:
    """
    DataExtractor class for extracting data from various sources including databases, PDFs, APIs, and AWS S3.
    This class provides methods to interface with different types of data storage and formats, facilitating
    the extraction and transformation of data into usable pandas DataFrames.
    """
    
    # API headers and URLs for extracting store data
    HEADER = {"x-api-key": ""}
    NUMBER_OF_STORES_URL = ""
    STORE_DETAILS_URL = ""
    S3_PATH = "s3://data-handling-public/products.csv"

    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """
        Reads a table from a relational database system (RDS) using a DatabaseConnector instance. 
        It checks for table existence in the database before attempting to read it.

        Args:
            db_connector (DatabaseConnector): An instance of DatabaseConnector for database connection.
            table_name (str): The name of the table to be read.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the specified table.

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
        Extracts data from a PDF file available at the specified URL. This method uses the 'tabula' library
        to read tables from the PDF and combines them into a single DataFrame.

        Args:
            link (str): URL of the PDF file.

        Returns:
            pd.DataFrame: A DataFrame combining all the tables extracted from the PDF.
        """
        # Extract and combine tables from the PDF into a single DataFrame
        dfs = tabula.read_pdf(
            link, pages="all", multiple_tables=True, lattice=True, stream=True
        )
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df

    def list_number_of_stores(self, url=NUMBER_OF_STORES_URL, headers=HEADER) -> dict:
        """
        Retrieves the number of stores from a specified API endpoint. This method sends a GET request to the API and
        returns the response in JSON format.

        Args:
            url (str, optional): API endpoint URL. Defaults to NUMBER_OF_STORES_URL.
            headers (dict, optional): Request headers. Defaults to HEADER.

        Returns:
            dict: A dictionary containing the API response.

        Raises:
            Exception: If the API request fails with a non-200 status code.
        """
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    def retrieve_stores_data(self, number_of_stores=451, base_url=STORE_DETAILS_URL, headers=HEADER) -> pd.DataFrame:
        """
        Retrieves details for a specified number of stores from an API. This method iteratively sends GET requests
        to the API for each store and compiles the results into a DataFrame.

        Args:
            number_of_stores (int, optional): Number of stores to retrieve. Defaults to 451.
            base_url (str, optional): Base URL for the API endpoint. Defaults to STORE_DETAILS_URL.
            headers (dict, optional): Request headers. Defaults to HEADER.

        Returns:
            pd.DataFrame: DataFrame containing details for each store.
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
        Extracts data from an AWS S3 bucket using the provided HTTP S3 URL. This method determines the file type
        (either CSV or JSON) from the URL and reads the data into a DataFrame accordingly.

        Args:
            s3_url (str): HTTP URL to the S3 file.

        Returns:
            pd.DataFrame: DataFrame containing data extracted from the S3 file.

        Raises:
            ValueError: If the file type is neither CSV nor JSON.
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

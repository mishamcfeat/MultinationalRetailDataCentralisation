from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import logging


class DataExtractor:

    API_KEY = "[REDACTED]"
    HEADER = {"x-api-key": API_KEY}
    NUMBER_OF_STORES_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    STORE_DETAILS_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

    def read_rds_table(self, db_connector, table_name):
        # Ensure db_connector is an instance of DatabaseConnector
        if not isinstance(db_connector, DatabaseConnector):
            raise TypeError("db_connector must be an instance of DatabaseConnector")

        # Initialise the database engine
        engine = db_connector.init_db_engine()

        # List all tables and check if the specified table exists
        available_tables = db_connector.list_db_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' not found in the database")

        # Extract the specified table into a pandas DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)
        return df

    def retrieve_pdf_data(self, link):
        # Extract tables from the PDF into a list of DataFrames
        dfs = tabula.read_pdf(link, pages='all', multiple_tables=True,
                              lattice=True, stream=True)

        # Combine all DataFrames into a single DataFrame
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df

    def list_number_of_stores(self, url=NUMBER_OF_STORES_URL, headers=HEADER):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    def retrieve_stores_data(self, number_of_stores=451, base_url=STORE_DETAILS_URL, headers=HEADER):        
        all_stores = []
        for store_number in range(1, number_of_stores + 1):
            try:
                url = f"{base_url}/{store_number}"
                response = requests.get(url, headers=headers)
                # Raise an HTTPError for bad responses
                response.raise_for_status()
                all_stores.append(response.json())
            except requests.HTTPError as e:
                # Logging doesn't halt the entire process on failed requests unlike raising Exceptions
                logging.error(f"Error retrieving data for store {store_number}: {e}")
        return pd.DataFrame(all_stores)


if __name__ == '__main__':
    extractor = DataExtractor()  # Create an instance of DataExtractor
    # df = extractor.read_rds_table(db_connector=DatabaseConnector('../config/db_creds.yaml'), table_name='legacy_users')
    # df = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    extractor = DataExtractor()
    number_of_stores = extractor.list_number_of_stores()
    print(number_of_stores)
    
    store_data = extractor.retrieve_stores_data(number_of_stores['number_stores'])
    store_data.to_csv('store_data.csv', index=False)
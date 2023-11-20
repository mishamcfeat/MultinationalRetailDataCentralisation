from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor


if __name__ == '__main__':
    # Step 1: Extract Data
    # extractor = DataExtractor()
    # df_extracted = extractor.retrieve_stores_data()

    # Step 2: Clean Data
    cleaner = DataCleaning()
    # df_cleaned = cleaner.clean_user_data(df_extracted)
    # df_cleaned = cleaner.clean_store_data('../csv/store_data.csv')
    df_cleaned = cleaner.clean_store_data('../csv/store_data.csv')

    # Step 3: Upload Cleaned Data
    # Assuming DatabaseConnector can connect to 'sales_data' database
    db_connector_store_data = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_store_data.upload_to_db(df_cleaned, 'dim_store_details')

    print("Data extraction, cleaning, and uploading process completed.")
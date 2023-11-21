from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor


if __name__ == '__main__':
    # Step 1: Extract Data
    extractor = DataExtractor()
    db_connector = DatabaseConnector('../config/db_creds.yaml')  # Assuming this connects to your source database
    df_extracted = extractor.read_rds_table(db_connector, 'orders_table')

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_orders_data(df_extracted)

    # Step 3: Upload Cleaned Data
    # Assuming DatabaseConnector can connect to 'sales_data' database
    db_connector_orders_data = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_orders_data.upload_to_db(df_cleaned, 'orders_table')

    print("Data extraction, cleaning, and uploading process completed.")
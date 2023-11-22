from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

if __name__ == '__main__':
    # Step 1: Extract Data
    extractor = DataExtractor()
    db_connector = DatabaseConnector('../config/db_creds.yaml')  # Connects to your source database
    df_extracted = extractor.read_rds_table(db_connector, 'orders_table')

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_orders_data(df_extracted)

    # Step 3: Upload Cleaned Data
    db_connector_orders_data = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_orders_data.upload_to_db(df=df_cleaned, table_name='orders_table', primary_key='order_id')

    # Step 4: Alter Column Data Types
    orders_table_mappings = {
        "order_id": "BIGINT",
        "date_uuid": "UUID USING date_uuid::uuid",
        "user_uuid": "UUID USING user_uuid::uuid",
        "card_number": "VARCHAR(19)",
        "store_code": "VARCHAR(12)",
        "product_code": "VARCHAR(11)",
        "product_quantity": "SMALLINT"
    }
    db_connector_orders_data.alter_column_data_types('orders_table', orders_table_mappings)

    print("Data extraction, cleaning, uploading, and data type alteration process completed.")
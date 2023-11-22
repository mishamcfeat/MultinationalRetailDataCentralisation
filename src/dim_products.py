from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import pandas as pd

if __name__ == '__main__':
    # Step 1: Extract Data
    # extractor = DataExtractor()
    # df_extracted = extractor.extract_from_s3("s3://data-handling-public/products.csv")
    df_extracted = pd.read_csv('../csv/product_data.csv')

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_converted = cleaner.convert_product_weights(df_extracted)
    df_cleaned = cleaner.clean_products_data(df_converted)

    # Step 3: Upload Cleaned Data
    db_connector_product_data = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_product_data.upload_to_db(df=df_cleaned, table_name='dim_products', primary_key='product_code')

    # Step 4: Alter Column Data Types
    dim_products_table_mappings = {
        "product_price_pounds": "FLOAT",
        "weight_kg": "FLOAT",
        "ean": "VARCHAR(17)",
        "product_code": "VARCHAR(11)",
        "date_added": "DATE",
        "uuid": "UUID USING uuid::uuid",
        "still_available": "BOOL",
        "weight_class": "VARCHAR(14)"
    }
    db_connector_product_data.alter_column_data_types('dim_products', dim_products_table_mappings)

    print("Data extraction, cleaning, uploading, and altering process completed.")
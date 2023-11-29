from src.database_utils import DatabaseConnector
from src.data_cleaning import DataCleaning
from src.data_extraction import DataExtractor

if __name__ == '__main__':
    # Step 1: Extract Data
    extractor = DataExtractor()
    db_connector = DatabaseConnector('../config/db_creds.yaml')
    df_extracted = extractor.read_rds_table(db_connector, 'legacy_users')

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_user_data(df_extracted)

    # Step 3: Alter Column Data Types
    db_connector_sales_data = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_sales_data.upload_to_db(df=df_cleaned, table_name='dim_users', primary_key='user_uuid')


    dim_users_table_mappings = {
        "first_name": "VARCHAR(255)",
        "last_name": "VARCHAR(255)",
        "date_of_birth": "DATE",
        "country_code": "VARCHAR(255)",
        "user_uuid": "UUID USING user_uuid::uuid",
        "join_date": "DATE"
    }
    db_connector_sales_data.alter_column_data_types('dim_users', dim_users_table_mappings)

    print("Data extraction, cleaning, uploading, and altering process completed.")
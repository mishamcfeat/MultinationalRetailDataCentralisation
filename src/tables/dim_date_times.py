from src.database_utils import DatabaseConnector
from src.data_cleaning import DataCleaning
from src.data_extraction import DataExtractor

if __name__ == '__main__':
    # Step 1: Extract Data
    extractor = DataExtractor()
    df_extracted = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_date_times(df_extracted)

    # Step 3: Upload Cleaned Data
    db_connector_date_times = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_date_times.upload_to_db(df=df_cleaned, table_name='dim_date_times', primary_key='date_uuid')

    # Step 4: Alter Column Data Types
    dim_date_times_table_mappings = {
        "timestamp": "TIME USING timestamp::time without time zone",
        "month": "VARCHAR(2)",
        "year": "VARCHAR(4)",
        "day": "VARCHAR(2)", 
        "time_period": "VARCHAR(10)",
        "date_uuid": "UUID USING date_uuid::uuid"
    }
    db_connector_date_times.alter_column_data_types('dim_date_times', dim_date_times_table_mappings)

    print("Data extraction, cleaning, and uploading process completed.")
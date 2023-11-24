from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor 

if __name__ == "__main__":
    # Step 1: Extract Data
    # extractor = DataExtractor()
    # df_extracted = extractor.retrieve_stores_data()

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_store_data('../csv/store_data.csv')

    # Step 3: Upload Cleaned Data
    db_connector_store_data = DatabaseConnector("../config/pgadmin_creds.yaml")
    db_connector_store_data.upload_to_db(df=df_cleaned, table_name='dim_store_details', primary_key='store_code')

    # # Step 4: Alter Column Data Types
    dim_store_details_mappings = {
        "longitude": "FLOAT USING longitude::double precision",
        "locality": "VARCHAR(255)",
        "store_code": "VARCHAR(12)",
        "staff_numbers": "SMALLINT",
        "opening_date": "DATE",
        "store_type": "VARCHAR(255)",
        "latitude": "FLOAT USING latitude::double precision",
        "country_code": "VARCHAR(2)",
        "continent": "VARCHAR(255)"
    }
    db_connector_store_data.alter_column_data_types(
        "dim_store_details", dim_store_details_mappings
    )

    print("Data extraction, cleaning, uploading, and altering process completed.")

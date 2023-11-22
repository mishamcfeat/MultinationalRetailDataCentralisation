from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import pandas as pd

if __name__ == '__main__':
    # Step 1: Extract Data
    extractor = DataExtractor()
    df_extracted = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # df_extracted = pd.read_csv('../csv/card_details.csv')
    
    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_card_data(df_extracted)

    # Step 3: Upload Cleaned Data
    db_connector_card_details = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_card_details.upload_to_db(df=df_cleaned, table_name='dim_card_details', primary_key='card_number')


    # Step 4: Alter Column Data Types
    dim_card_details_table_mappings = {
        "card_number": "VARCHAR(19)",  # Card numbers are typically 16 digits, but can vary up to 19
        "expiry_date": "VARCHAR(10)",  # Dates in format YYYY-MM-DD need 10 characters
        "date_payment_confirmed": "DATE USING date_payment_confirmed::date"  # Date type is suitable for this column
    }
    db_connector_card_details.alter_column_data_types('dim_card_details', dim_card_details_table_mappings)

    print("Data extraction, cleaning, and uploading process completed.")

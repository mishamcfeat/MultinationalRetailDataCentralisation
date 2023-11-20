from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor


if __name__ == '__main__':
    # Step 1: Extract Data
    extractor = DataExtractor()
    df_extracted = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    # Step 2: Clean Data
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_card_data(df_extracted)

    # Step 3: Upload Cleaned Data
    # Assuming DatabaseConnector can connect to 'sales_data' database
    db_connector_sales_data = DatabaseConnector('../config/pgadmin_creds.yaml')
    db_connector_sales_data.upload_to_db(df_cleaned, 'dim_card_details')

    print("Data extraction, cleaning, and uploading process completed.")

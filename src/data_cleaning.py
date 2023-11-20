import pandas as pd
import numpy as np


class DataCleaning():
    def clean_user_data(self, df):

        # Set the 'index' column as the DataFrame index
        df.set_index('index', inplace=True)

        # Convert 'date_of_birth' and 'join_date' to datetime without time, handling errors
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.date
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.date

        df.loc[df['country'] == 'United Kingdom', 'country_code'] = 'GB'

        # Regex pattern for UK phone numbers
        uk_regex = r'^(?:(?:\+44\s?\(0\)\s?\d{2,4}|\(?\d{2,5}\)?)\s?\d{3,4}\s?\d{3,4}$|\d{10,11}|\+44\s?\d{2,5}\s?\d{3,4}\s?\d{3,4})$'
        de_regex = r'(\(?([\d \-\)\–\+\/\(]+){6,}\)?([ .\-–\/]?)([\d]+))'
        us_regex = r'\(?\d{3}\)?-? *\d{3}-? *-?\d{4}'

        # Replace non-conforming UK phone numbers with NaN
        df.loc[(df['country_code'] == 'GB') & (~df['phone_number'].astype(str).str.match(uk_regex)), 'phone_number'] = np.nan
        df.loc[(df['country_code'] == 'DE') & (~df['phone_number'].astype(str).str.match(de_regex)), 'phone_number'] = np.nan
        df.loc[(df['country_code'] == 'US') & (~df['phone_number'].astype(str).str.match(us_regex)), 'phone_number'] = np.nan

        # Correct data types for other columns
        df['country_code'] = df['country_code'].astype(str)
        df['phone_number'] = df['phone_number'].astype(str)
        df['email_address'] = df['email_address'].astype(str)
        df['address'] = df['address'].astype(str)

        # Handle missing values - remove all rows with any null values
        df.dropna(inplace=True)
        return df

    def clean_card_data(self, df):

        # Replace 'NULL' strings with NaN
        df.replace('NULL', np.nan, inplace=True)
        
        # Drop rows with any null values
        df.dropna(inplace=True)

        # Convert date columns to a uniform format, coercing errors to NaT
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format="%m/%y", errors='coerce').dt.date
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce').dt.date

        # Drop rows with any null values
        df.dropna(inplace=True)
        return df

    def clean_store_data(self, csv_file):
        # Read data from CSV
        df = pd.read_csv(csv_file)

        # Apply your cleaning operations here
        # Example: Replace 'NULL' strings with NaN and drop rows with null values
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)

        return df
    


if __name__ == '__main__':
    cleaner = DataCleaning()
    cleaned_data = cleaner.clean_store_data('store_data.csv')
    # Display the first few rows of the cleaned data
    print(cleaned_data.head())

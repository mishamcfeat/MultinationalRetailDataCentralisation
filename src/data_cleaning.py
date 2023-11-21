import pandas as pd
import numpy as np
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import re


class DataCleaning:
    def clean_user_data(self, df):
        """
        Cleans user data DataFrame.

        - Sets 'index' column as DataFrame index.
        - Converts 'date_of_birth' and 'join_date' to datetime format.
        - Updates 'country_code' for 'United Kingdom' entries.
        - Validates phone numbers based on country code.
        - Drops rows with any null values.
        """
        # Set the 'index' column as the DataFrame index for easier data manipulation
        df.set_index("index", inplace=True)

        # Convert 'date_of_birth' and 'join_date' to datetime format, handling parsing errors
        df["date_of_birth"] = pd.to_datetime(
            df["date_of_birth"], errors="coerce"
        ).dt.date
        df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce").dt.date

        # Update 'country_code' for entries from the United Kingdom
        df.loc[df["country"] == "United Kingdom", "country_code"] = "GB"

        # Define regular expressions for phone number validation
        uk_regex = r"^(?:(?:\+44\s?\(0\)\s?\d{2,4}|\(?\d{2,5}\)?)\s?\d{3,4}\s?\d{3,4}$|\d{10,11}|\+44\s?\d{2,5}\s?\d{3,4}\s?\d{3,4})$"
        de_regex = r"(\(?([\d \-\)\–\+\/\(]+){6,}\)?([ .\-–\/]?)([\d]+))"
        us_regex = r"\(?\d{3}\)?-? *\d{3}-? *-?\d{4}"

        # Filter out invalid phone numbers based on regex patterns
        df.loc[
            (df["country_code"] == "GB")
            & (~df["phone_number"].astype(str).str.match(uk_regex)),
            "phone_number",
        ] = np.nan
        df.loc[
            (df["country_code"] == "DE")
            & (~df["phone_number"].astype(str).str.match(de_regex)),
            "phone_number",
        ] = np.nan
        df.loc[
            (df["country_code"] == "US")
            & (~df["phone_number"].astype(str).str.match(us_regex)),
            "phone_number",
        ] = np.nan

        # Ensure correct data types for other columns
        df["country_code"] = df["country_code"].astype(str)
        df["phone_number"] = df["phone_number"].astype(str)
        df["email_address"] = df["email_address"].astype(str)
        df["address"] = df["address"].astype(str)

        # Remove rows with any null values
        df.dropna(inplace=True)
        return df

    def clean_card_data(self, df):
        """
        Cleans card data DataFrame.

        - Replaces 'NULL' strings with NaN.
        - Converts date columns ('expiry_date', 'date_payment_confirmed') to uniform datetime format.
        - Drops rows with any null values.
        """
        # Replace 'NULL' strings with NaN for consistent missing value handling
        df.replace("NULL", np.nan, inplace=True)

        # Drop rows with any null values
        df.dropna(inplace=True)

        # Standardize date formats, converting to datetime and handling parsing errors
        df["expiry_date"] = pd.to_datetime(
            df["expiry_date"], format="%m/%y", errors="coerce"
        ).dt.date
        df["date_payment_confirmed"] = pd.to_datetime(
            df["date_payment_confirmed"], errors="coerce"
        ).dt.date

        # Remove rows with any null values after date conversion
        df.dropna(inplace=True)
        return df

    def clean_store_data(self, csv_file):
        """
        Cleans store data from a CSV file.

        - Reads data and sets 'index' as DataFrame index.
        - Removes invalid country codes and corrects continent names.
        - Processes staff numbers and location coordinates.
        - Converts various columns to appropriate data types.
        - Handles missing values.
        """
        # Read data from CSV and set 'index' column as DataFrame index
        df = pd.read_csv(csv_file)
        df.set_index("index", inplace=True)

        # Remove 'lat' column (assuming it's redundant or not needed)
        df.drop(columns=["lat"], inplace=True)

        # Replace 'NULL' strings with NaN for consistent missing value handling
        df.replace("NULL", np.nan, inplace=True)

        # Replace invalid country codes with NaN
        df.loc[~df["country_code"].isin(["DE", "US", "GB"]), "country_code"] = np.nan

        # Correct misspelled continent names
        df.loc[df["continent"] == "eeEurope", "continent"] = "Europe"
        df.loc[df["continent"] == "eeAmerica", "continent"] = "America"

        # Process staff numbers, extracting numeric part and handling non-numeric entries
        df["staff_numbers"] = pd.to_numeric(
            df["staff_numbers"].str.extract("(\d+)", expand=False), errors="coerce"
        )

        # Drop rows with missing values
        df.dropna(inplace=True)

        # Convert latitude and longitude to float and round to 5 decimal places
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").round(5)
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").round(5)

        # Convert various columns to string (object) type for consistency
        df["address"] = df["address"].astype(str)
        df["locality"] = df["locality"].astype(str)
        df["store_code"] = df["store_code"].astype(str)
        df["store_type"] = df["store_type"].astype(str)
        df["country_code"] = df["country_code"].astype(str)
        df["continent"] = df["continent"].astype(str)

        # Convert staff_numbers to integer type
        df["staff_numbers"] = df["staff_numbers"].astype(int)

        # Convert 'opening_date' to datetime and extract date part
        df["opening_date"] = pd.to_datetime(
            df["opening_date"], errors="coerce", format="mixed"
        ).dt.date

        # Drop rows with missing values after conversions
        df.dropna(inplace=True)
        return df

    def convert_product_weights(self, df):
        """
        Converts product weights to kilograms. Handles weights in various formats,
        including those with multiplication (e.g., '5 x 145g') and different units (g, ml).
        Assumes a 1:1 ratio for ml to g for conversion.

        Args:
            df (pandas.DataFrame): DataFrame containing product data with a 'weight' column.

        Returns:
            pandas.DataFrame: Updated DataFrame with a new column 'weight_kg' representing weights in kg.
        """

        def convert_to_kg(weight_str):
            """
            Converts a weight string to kilograms.
            Handles multiplication and different units (g, ml).

            Args:
                weight_str (str): Weight string from the DataFrame.

            Returns:
                float: Weight converted to kilograms, or None if conversion is not possible.
            """
            if not isinstance(weight_str, str) or pd.isna(weight_str):
                return None

            # Handle multiplication format, e.g., '5 x 145g'
            if "x" in weight_str:
                parts = weight_str.split("x")
                try:
                    quantity = float(re.findall(r"(\d+(\.\d+)?)", parts[0])[0][0])
                    unit_weight = float(re.findall(r"(\d+(\.\d+)?)", parts[1])[0][0])
                    total_weight = quantity * unit_weight
                except (IndexError, ValueError):
                    return None
            else:
                # Extract numeric weight from string
                numeric_match = re.findall(r"(\d+(\.\d+)?)", weight_str)
                if numeric_match:
                    total_weight = float(numeric_match[0][0])
                else:
                    return None

            # Convert grams and milliliters to kilograms
            if "g" in weight_str or "ml" in weight_str:
                total_weight /= 1000

            return total_weight

        df["weight_kg"] = df["weight"].apply(convert_to_kg)
        # Drop the original 'weight' column
        df.drop(columns=["weight"], inplace=True)
        # Convert 'weight_kg' to numeric and round to 5 decimal places
        df["weight_kg"] = pd.to_numeric(df["weight_kg"], errors="coerce").round(5)

        return df
    
    def clean_products_data(self, df):
        """
        Cleans the DataFrame of any additional erroneous values.

        Args:
            df (pandas.DataFrame): DataFrame containing product data.

        Returns:
            pandas.DataFrame: Cleaned DataFrame.
        """

        # Remove duplicates
        df = df.drop_duplicates()
        
        df.set_index('Unnamed: 0', inplace=True)
        
        df.replace("NULL", np.nan, inplace=True)
        df.dropna(inplace=True)

        # Standardize text fields (example: 'product_name' column)
        if 'product_name' in df.columns:
            df['product_name'] = df['product_name'].str.strip().str.lower()

        # Remove pound symbol from 'Price' and convert to numeric
        if 'product_price' in df.columns:
            df['product_price'] = df['product_price'].str.replace('£', '').str.strip()
            df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce').round(2)

        df.rename(columns={'product_price': 'product_price_pounds'}, inplace=True)

        df["date_added"] = pd.to_datetime(
            df["date_added"], errors="coerce"
        ).dt.date

        return df

    def clean_orders_data(self, df):
        """
        Cleans the orders table data by removing specified columns,
        setting the index, replacing 'NULL' values with NaN, and 
        converting data types where necessary.

        Parameters:
        df (DataFrame): A pandas DataFrame containing the orders data.

        Returns:
        DataFrame: A cleaned DataFrame.
        """

        # Remove the specified columns
        df.drop(columns=["first_name", "last_name", "1", "level_0"], inplace=True, errors='ignore')

        # Set 'index' as the new DataFrame index
        df.set_index('index', inplace=True)

        # Replace 'NULL' strings with NaN
        df.replace("NULL", np.nan, inplace=True)

        # Drop rows with NaN values
        df.dropna(inplace=True)

        # Convert 'product_quantity' to int
        df['product_quantity'] = df['product_quantity'].astype(int)

        return df

    def clean_date_times(self, df):
        """
        Cleans the date_times data by filtering out unwanted time periods, 
        combining year, month, day, and timestamp into a single 'date_time' 
        column with standard y-m-d H:M:S formatting, and removing rows with 
        NULL values.

        Parameters:
        df (DataFrame): A pandas DataFrame containing the date_times data.

        Returns:
        DataFrame: A cleaned DataFrame.
        """

        # Replace 'NULL' strings with NaN
        df.replace("NULL", np.nan, inplace=True)

        # Filter rows where time period is not in the specified list and create a copy
        valid_time_periods = ['Late_Hours', 'Morning', 'Midday', 'Evening']
        df = df[df['time_period'].isin(valid_time_periods)].copy()

        # Drop rows with NaN values in key columns
        df.dropna(subset=['year', 'month', 'day', 'timestamp', 'time_period'], inplace=True)

        # Combine year, month, day, and timestamp into one 'date_time' column
        df['date_time'] = pd.to_datetime({
            'year': df['year'],
            'month': df['month'],
            'day': df['day'],
            'hour': df['timestamp'].str.split(':').str[0],
            'minute': df['timestamp'].str.split(':').str[1],
            'second': df['timestamp'].str.split(':').str[2]
        })

        # Drop the original year, month, day, timestamp, and time_period columns
        df.drop(columns=['year', 'month', 'day', 'timestamp', 'time_period'], inplace=True)

        return df


if __name__ == "__main__":
    extractor = DataExtractor()
    df = extractor.read_rds_table(db_connector=DatabaseConnector('../config/db_creds.yaml'), table_name='orders_table')
    clean = DataCleaning()
    df_cleaned = clean.clean_orders_data(df)
    print(df_cleaned.head())

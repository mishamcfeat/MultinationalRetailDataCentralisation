import pandas as pd
import numpy as np


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

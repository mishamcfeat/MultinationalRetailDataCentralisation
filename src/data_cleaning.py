from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import numpy as np
import re
import uuid


class DataCleaning:
    """
    The DataCleaning class provides methods to clean and standardize various types of data including user,
    card, store, and product data. It handles tasks like format normalization, data type conversions,
    removal of invalid entries, and setting appropriate data types.
    """
    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the user data DataFrame by performing operations like removing null values, converting date columns
        to datetime format, standardizing country codes, validating phone numbers, and ensuring valid UUIDs. It
        also filters out entries with invalid or incomplete data.

        Args:
            df (pd.DataFrame): DataFrame containing user data.

        Returns:
            pd.DataFrame: Cleaned and standardized user data DataFrame.
        """
        # Removes null values and sets index column
        self._clean_dataframe(df=df, index="index")

        # Convert 'date_of_birth' & 'join_date' to datetime format
        df["date_of_birth"] = pd.to_datetime(
            df["date_of_birth"], format="mixed", errors="coerce"
        ).dt.date
        df["join_date"] = pd.to_datetime(
            df["join_date"], format="mixed", errors="coerce"
        ).dt.date

        # Correct errors in 'country_code' for entries from the United Kingdom
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

        # Filter out entries with invalid 'user_uuid'
        df = df[df["user_uuid"].apply(lambda x: is_valid_uuid(x))]

        # Filter out or correct invalid 'country_code' entries
        # Assuming 'country_code' should be 2 letters. Modify as needed.
        df = df[df["country_code"].str.len() == 2]

        # Remove rows with any null values
        df.dropna(inplace=True)

        return df

    def clean_card_data(self, csv_file: str) -> pd.DataFrame:
        """
        Reads and cleans card data from a CSV file. It includes operations like reading data, removing null values,
        standardizing date formats, correcting invalid card numbers, and dropping rows with missing values after
        these operations.

        Args:
            csv_file (str): Path to the CSV file containing card data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned card data.
        """
        # Read data from CSV
        df = pd.read_csv(csv_file)
        
        # Removes null values and sets index column
        self._clean_dataframe(df=df, index='Unnamed: 0')
        
        # Standardise date formats, converting to datetime format
        df["date_payment_confirmed"] = pd.to_datetime(
            df["date_payment_confirmed"], format="mixed", errors="coerce"
        ).dt.date

        # Correct invalid card numbers by removing all leading question marks
        df["card_number"] = df["card_number"].str.replace(r"^\?+", "", regex=True)
        
        df.dropna(inplace=True)
        
        return df

    def clean_store_data(self, csv_file: str) -> pd.DataFrame:
        """
        Cleans store data from a CSV file. This includes reading the data, dropping irrelevant columns,
        filling null values, standardizing country codes, correcting misspelled continent names, processing staff
        numbers, and handling date formats. It drops rows with missing values after these operations.

        Args:
            csv_file (str): Path to the CSV file containing store data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned store data.
        """
        # Read data from CSV
        df = pd.read_csv(csv_file)

        # Drop the faulty 'lat' column
        df.drop(columns="lat", inplace=True)

        # Fill null values in the first row (online store) with 'N/A'
        df.iloc[0] = df.iloc[0].fillna("N/A")

        self._clean_dataframe(df=df, index="index")

        # Replace invalid country codes with NaN
        df.loc[~df["country_code"].isin(["DE", "US", "GB"]), "country_code"] = np.nan

        # Correct misspelled continent names
        df.loc[df["continent"] == "eeEurope", "continent"] = "Europe"
        df.loc[df["continent"] == "eeAmerica", "continent"] = "America"

        # Process staff numbers
        df["staff_numbers"] = pd.to_numeric(
            df["staff_numbers"].str.extract("(\d+)", expand=False), errors="coerce"
        )

        # Convert latitude and longitude to object type to accommodate N/A values
        df["latitude"] = df["latitude"].astype(object)
        df["longitude"] = df["longitude"].astype(object)

        # Convert 'opening_date' to datetime
        df["opening_date"] = pd.to_datetime(
            df["opening_date"], format="mixed", errors="coerce"
        ).dt.date

        # Drop rows with missing values after conversions, except for the website entry
        df.dropna(inplace=True)

        # Replace 'N/A' with NaN
        df.replace("N/A", np.nan, inplace=True)

        return df

    def convert_product_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts product weights to kilograms from various formats such as grams, milliliters, or expressions 
        involving multiplication (e.g., '5 x 145g'). It creates a new column 'weight_kg' and drops the original 
        'weight' column.

        Args:
            df (pandas.DataFrame): DataFrame containing product data with a 'weight' column.

        Returns:
            pandas.DataFrame: Updated DataFrame with weights converted to kg in the 'weight_kg' column.
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

    def clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans product data in the DataFrame by standardizing text fields, converting price formats, renaming columns
        for clarity, converting boolean fields, and validating UUIDs. It also includes operations for date formatting
        and categorizing products based on weight.

        Args:
            df (pd.DataFrame): DataFrame containing product data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned product data.
        """

        # Removes null values and sets index column
        self._clean_dataframe(df=df, index="Unnamed: 0")

        # Standardize text fields (example: 'product_name' column)
        if "product_name" in df.columns:
            df["product_name"] = df["product_name"].str.strip().str.lower()

        # Remove pound symbol from 'Price' and convert to numeric
        if "product_price" in df.columns:
            df["product_price"] = df["product_price"].str.replace("£", "").str.strip()
            df["product_price"] = pd.to_numeric(
                df["product_price"], errors="coerce"
            ).round(2)

        df.rename(columns={"product_price": "product_price_pounds"}, inplace=True)
        df.rename(columns={"removed": "still_available"}, inplace=True)
        df.rename(columns={"EAN": "ean"}, inplace=True)

        # Convert 'still_available' to boolean based on text
        df["still_available"] = np.where(
            df["still_available"] == "Still_avaliable", True, False
        )

        df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce").dt.date

        # Add weight_class column based on weight range
        conditions = [
            df["weight_kg"] < 2,
            df["weight_kg"].between(2, 40, inclusive="left"),
            df["weight_kg"].between(40, 140, inclusive="left"),
            df["weight_kg"] >= 140,
        ]
        choices = ["Light", "Mid_Sized", "Heavy", "Truck_Required"]
        df["weight_class"] = np.select(conditions, choices, default="Unknown")

        # Validate and filter UUIDs
        uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        df = df[df["uuid"].apply(lambda x: bool(re.match(uuid_pattern, str(x))))]

        return df

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the orders DataFrame by removing specified columns, setting the index, replacing 'NULL' values with NaN,
        and ensuring all entries have valid UUIDs. It drops rows with missing values after these operations.

        Args:
            df (pd.DataFrame): DataFrame containing orders data.

        Returns:
            pd.DataFrame: Cleaned orders DataFrame.
        """

        # Remove the specified columns
        df.drop(
            columns=["first_name", "last_name", "1"],
            inplace=True,
            errors="ignore",
        )

        # Removes null values and sets index column
        self._clean_dataframe(df=df, index="index")

        # Filter out entries with invalid 'user_uuid'
        df = df[df["user_uuid"].apply(lambda x: is_valid_uuid(x))]

        df.rename(columns={"level_0": "order_id"}, inplace=True)

        # Drop rows with missing values
        df.dropna(inplace=True)

        return df

    def clean_date_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the date_times DataFrame by removing null values, validating time periods against a predefined list,
        and setting an appropriate index. 

        Args:
            df (pd.DataFrame): DataFrame containing date_times data.

        Returns:
            pd.DataFrame: Cleaned date_times DataFrame.
        """
        # Removes null values and sets index column
        self._clean_dataframe(df=df)

        # Filter rows where time period is not in the specified list
        valid_time_periods = ["Late_Hours", "Morning", "Midday", "Evening"]
        df = df[df["time_period"].isin(valid_time_periods)]

        return df

    def _clean_dataframe(self, df: pd.DataFrame, index: str = None) -> None:
        """
        A helper method to perform basic cleaning operations on a DataFrame. It replaces 'NULL' strings with NaN,
        removes duplicates, drops rows with NaN values, and sets a specified column as the DataFrame index.

        Args:
            df (pd.DataFrame): DataFrame to be cleaned.
            index (str, optional): Column name to set as DataFrame index. Defaults to None.
        """
        # Replace 'NULL' strings with NaN, remove duplicates, and drop rows with NaN values
        df.replace("NULL", np.nan, inplace=True)
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        
        # Set 'index' as the new DataFrame index if provided
        if index and index in df.columns:
            df.set_index(index, inplace=True)


def is_valid_uuid(uuid_to_test, version=4):
    """
    Validates whether the given string is a valid UUID of the specified version. It checks the format and the
    version of the UUID.

    Args:
        uuid_to_test (str): The string to test for being a valid UUID.
        version (int): The UUID version. Default is 4.

    Returns:
        bool: True if uuid_to_test is a valid UUID, otherwise False.
    """
    try:
        uuid_obj = uuid.UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


if __name__ == "__main__":
    cleaner = DataCleaning()
    df_cleaned = cleaner.clean_store_data("../csv_files/store_data.csv")
    print(df_cleaned.head())

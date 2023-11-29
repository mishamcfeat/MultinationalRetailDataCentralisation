from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List
import pandas as pd
import yaml


class DatabaseConnector:
    """
    A class to handle database connections and operations. It supports reading database credentials,
    initializing database connections, executing various database operations like listing tables,
    altering column data types, uploading data, and executing custom SQL commands.

    Attributes:
        creds (dict[str, str]): Database credentials.
    """

    def __init__(self, file_name: str):
        """
        Initialises the DatabaseConnector with credentials read from a specified YAML file.

        Args:
            file_name (str): Path to the YAML file containing database credentials.
        """
        self.creds = self.read_db_creds(file_name)

    def read_db_creds(self, file_name: str) -> Dict[str, str]:
        """
        Reads database credentials from a YAML file and returns them as a dictionary.

        Args:
            file_name (str): Path to the YAML file.

        Returns:
            dict[str, str]: Database credentials.
        """
        with open(file_name, 'r') as f:
            creds = yaml.safe_load(f)
        return creds

    def init_db_engine(self) -> create_engine:
        """
        Initialises and returns a SQLAlchemy engine for database interaction using stored credentials.
        The engine is configured for a PostgreSQL database using psycopg2 as the DBAPI.

        Returns:
            create_engine: A SQLAlchemy engine instance configured for the database.
        """
        creds = self.creds
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self) -> List[str]:
        """
        Lists all tables in the connected database by querying the database metadata.

        Returns:
            List[str]: A list of table names available in the database.
        """
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def alter_column_data_types(self, table_name: str, column_type_mappings: dict) -> None:
        """
        Alters the data types of specified columns in a given table. The method iteratively
        executes ALTER TABLE commands for each column and its corresponding new data type.

        Args:
            table_name (str): The name of the table to alter.
            column_type_mappings (dict): A dictionary mapping column names to their new data types.

        Raises:
            SQLAlchemyError: If an error occurs during the alteration process.
        """
        try:
            engine = self.init_db_engine()
            with engine.connect() as conn:
                for column, new_type in column_type_mappings.items():
                    conn.execute(f"ALTER TABLE {table_name} ALTER COLUMN {column} TYPE {new_type};")
            print(f"Data types altered successfully for table '{table_name}'")
        except SQLAlchemyError as e:
            print(f"An error occurred while altering data types in table '{table_name}':", e)

    def upload_to_db(self, df: pd.DataFrame, table_name: str, primary_key: str = None) -> None:
        """
        Uploads a DataFrame to a specified table in the database. If the table exists, it is replaced.
        Optionally sets a primary key on the specified column after the upload.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the target table in the database.
            primary_key (str, optional): The column name to be set as the primary key.

        Raises:
            ValueError: If df is not a pandas DataFrame.
            SQLAlchemyError: If an error occurs during the upload process.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame")

        try:
            engine = self.init_db_engine()
            with engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Data uploaded successfully to table '{table_name}'")
                if primary_key:
                    conn.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key});")
                    print(f"Primary key '{primary_key}' set for table '{table_name}'")
        except SQLAlchemyError as e:
            print("An error occurred while uploading data to the database:", e)
            
    def execute_sql(self, sql: str) -> None:
        """
        Executes a custom SQL command in the connected database. This method is useful for database operations
        that are not covered by other methods in this class.

        Args:
            sql (str): The SQL command to execute.

        Raises:
            SQLAlchemyError: If an error occurs during the execution.
        """
        try:
            engine = self.init_db_engine()
            with engine.connect() as conn:
                conn.execute(sql)
            print(f"SQL command executed successfully.")
        except SQLAlchemyError as e:
            print(f"An error occurred while executing SQL command:", e)


if __name__ == '__main__':
    db = DatabaseConnector('../config/db_creds.yaml')
    print(db.list_db_tables())
    # Example: db.upload_to_db(df=your_dataframe, table_name='legacy_users')
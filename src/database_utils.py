import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List


class DatabaseConnector:
    """
    A class to handle database connections and operations.

    Attributes:
        creds (Dict[str, str]): Database credentials.
    """

    def __init__(self, file_name: str):
        """
        Initializes the DatabaseConnector with credentials from a YAML file.

        Args:
            file_name (str): Path to the YAML file containing database credentials.
        """
        self.creds = self.read_db_creds(file_name)

    def read_db_creds(self, file_name: str) -> Dict[str, str]:
        """
        Reads database credentials from a YAML file.

        Args:
            file_name (str): Path to the YAML file.

        Returns:
            Dict[str, str]: Database credentials.
        """
        with open(file_name, 'r') as f:
            creds = yaml.safe_load(f)
        return creds

    def init_db_engine(self) -> create_engine:
        """
        Initializes and returns a SQLAlchemy engine using stored credentials.

        Returns:
            create_engine: A SQLAlchemy engine instance.
        """
        creds = self.creds
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self) -> List[str]:
        """
        Lists all tables in the connected database.

        Returns:
            List[str]: A list of table names.
        """
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Uploads a DataFrame to a specified table in the database.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the target table in the database.

        Raises:
            ValueError: If df is not a pandas DataFrame.
            SQLAlchemyError: If an error occurs during the upload process.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame")

        try:
            engine = self.init_db_engine()
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to table '{table_name}'")
        except SQLAlchemyError as e:
            print("An error occurred while uploading data to the database:", e)


if __name__ == '__main__':
    db = DatabaseConnector('../config/db_creds.yaml')
    print(db.list_db_tables())
    # Example: db.upload_to_db(df=your_dataframe, table_name='legacy_users')
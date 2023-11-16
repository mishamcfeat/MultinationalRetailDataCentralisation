import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


class DatabaseConnector:
    def __init__(self, file_name):
        # Read database credentials upon initialization
        self.creds = self.read_db_creds(file_name)

    def read_db_creds(self, file_name):
        # Reads YAML file credentials
        with open(file_name, 'r') as f:
            creds = yaml.safe_load(f)
        return creds

    def init_db_engine(self):
        # Connects to the database using the stored credentials
        creds = self.creds
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        # Lists all tables from the database using the initialized engine
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        """
        Uploads a DataFrame to the specified table in the database.

        :param df: The DataFrame to upload.
        :param table_name: The name of the target table in the database.
        :raises: ValueError if df is not a DataFrame.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame")

        try:
            # Initialize database engine
            engine = self.init_db_engine()

            # Upload DataFrame to the database
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to table '{table_name}'")
        except SQLAlchemyError as e:
            print("An error occurred while uploading data to the database:", e)


if __name__ == '__main__':
    db = DatabaseConnector()
    print(db.list_db_tables())
    #db.upload_to_db(df=DataExtractor(), table_name='legacy_users')

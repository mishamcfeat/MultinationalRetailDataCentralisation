from database_utils import DatabaseConnector
import pandas as pd


class DataExtractor:
    def read_rds_table(self, db_connector, table_name):
        # Ensure db_connector is an instance of DatabaseConnector
        if not isinstance(db_connector, DatabaseConnector):
            raise TypeError("db_connector must be an instance of DatabaseConnector")

        # Initialise the database engine
        engine = db_connector.init_db_engine()

        # List all tables and check if the specified table exists
        available_tables = db_connector.list_db_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' not found in the database")

        # Extract the specified table into a pandas DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)
        return df


if __name__ == '__main__':
    extractor = DataExtractor()  # Create an instance of DataExtractor
    df = extractor.read_rds_table(db_connector=DatabaseConnector('db_creds.yaml'), table_name='legacy_users')
    print(df)
    #df.to_csv('legacy_users.csv')

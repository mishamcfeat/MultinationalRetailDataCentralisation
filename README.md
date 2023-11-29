# Multinational Retail Data Centralisation

## Overview
This project centralizes retail data from multiple international branches of our retail chain into a unified database. The aim is to streamline data analysis and reporting across different regions, ensuring consistency and accuracy in our retail insights.

## Features
- **Data Extraction**: Automated extraction from diverse sources including SQL databases, CSV files, and web APIs.
- **Data Cleaning**: Robust preprocessing to handle inconsistencies, missing values, and outliers.
- **Database Management**: Efficient storage and retrieval using a centralised SQL database.

## Installation
1. Clone the repository: `git clone [repository-url]`
2. Install required dependencies: `pip install -r requirements.txt`
3. Set up your database credentials in config folder with yaml files e.g. `config.yaml`

## Usage
- To extract data: Use methods from the DataExtractor class in `data_extraction.py`
- For data cleaning: Use methods from the DataCleaning class in `data_cleaning.py`
- To upload data to the database: Use methods from the DatabaseConnector class in `database_utils.py`

## File Structure

- `README.md`: The main documentation file for the project.
- `/config`: Contains configuration files.
  - `db_creds.yaml`: Database credentials configuration.
  - `pgadmin_creds.yaml`: PGAdmin credentials configuration.
- `/csv`: Folder containing CSV data files.
  - `card_details.csv`: Contains card details.
  - `store_data.csv`: Store-related data.
  - `legacy_users.csv`: Data of legacy users.
  - `product_data.csv`: Product-related data.
- `/src`: Source code of the project.
  - `data_cleaning.py`: Script for cleaning data.
  - `data_extraction.py`: Script for extracting data from various sources.
  - `database_utils.py`: Utilities for database operations.
  - `/tables`: Contains scripts for database table definitions.
    - `dim_products.py`: Product dimension table script.
    - `dim_card_details.py`: Card details dimension table script.
    - `dim_store_details.py`: Store details dimension table script.
    - `dim_users.py`: User dimension table script.
    - `dim_date_times.py`: Date Times dimension table script.
    - `orders_table.py`: Orders dimension table script.
- `/text`: Text files containing Foreign Key constraints and SQL queries
 - `DataQueries.txt`: Contains all queries for Milestone 4: Querying the data.
 - `SQLconstraints.txt`: Contains all Foregin Key constraints for Orders table
- `requirements.txt`: Lists all the Python dependencies for the project.

## Contributing
We welcome contributions to this project. Please fork the repository, make your changes, and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


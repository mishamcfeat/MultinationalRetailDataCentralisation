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
3. Set up your database credentials in config folder with yaml files e.g. `pgadmin_creds.yaml`

## Usage
- To extract data: Use methods from the DataExtractor class in `data_extraction.py`
- For data cleaning: Use methods from the DataCleaning class in `data_cleaning.py`
- To upload data to the database: Use methods from the DatabaseConnector class in `database_utils.py`
- Set up the Foreign Key Constraints using the Foreign Keys folder files.
- Query as required to collect up-to-date metrics of the data

## File Structure

- `README.md`: The main documentation file for the project.
- `/config`: Contains configuration files.
  - `db_creds.yaml`: Database credentials configuration.
  - `pgadmin_creds.yaml`: PGAdmin credentials configuration.
- `/csv_files`: Folder containing CSV data files.
  - `card_details.csv`: Contains card details.
  - `store_data.csv`: Store-related data.
  - `user_data.csv`: Data of legacy users.
  - `product_data.csv`: Product-related data.
  - `date_times.csv`: Purchase data related data.
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
- `/sql`: SQL files containing Foreign Key constraints and query tasks.
  - `/ForeignKeys`: Contains SQL scripts for foreign key constraints.
    - `fk_card_number.sql`: Foreign key constraint for card number.
    - `fk_date_uuid.sql`: Foreign key constraint for date UUID.
    - `fk_product_code.sql`: Foreign key constraint for product code.
    - `fk_store_code.sql`: Foreign key constraint for store code.
    - `fk_user_uuid.sql`: Foreign key constraint for user UUID.
  - `/QueryTasks`: Contains SQL scripts for specific query tasks.
    - `Task1.sql`: Query for Task 1.
    - `Task2.sql`: Query for Task 2.
    - `Task3.sql`: Query for Task 3.
    - `Task4.sql`: Query for Task 4.
    - `Task5.sql`: Query for Task 5.
    - `Task6.sql`: Query for Task 6.
    - `Task7.sql`: Query for Task 7.
    - `Task8.sql`: Query for Task 8.
    - `Task9.sql`: Query for Task 9.
    - `Task9v2.sql`: Second version of the query for Task
- `requirements.txt`: Lists all the Python dependencies for the project.

## Contributing
Please fork the repository, make your changes, and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


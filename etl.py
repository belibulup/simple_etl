import pandas as pd
from sqlalchemy import create_engine

print("Starting ETL script...")

def extract(file_path):
    """
    Extract : Read data from CSV file
    """
    return pd.read_csv(file_path)

def transform(data):
    """
    Transform: Clean and process the data
    """
    # Basic cleaning eg. drop rows with missing values
    data.dropna(inplace=True)
    # Converting string to integer for age / data type
    data['age'] = data['age'].astype(int)
    return data

def load(data, db_file):
    """
    Load: Save the data to SQLite database
    """
    engine = create_engine(f'sqlite:///{db_file}')
    data.to_sql('people', engine, if_exists='replace', index=False)
    print(f"Data loaded into database '{db_file}'")

def main():
    csv_file = 'data/data.csv'
    db_file = "output/database.db"

    # Extract
    data = extract(csv_file)
    print("Data extracted")

    # Transform
    data = transform(data)
    print("Data transformed")

    # Load
    load(data, db_file)
    print("ETL process completed.")

if __name__ == "__main__":
    main()

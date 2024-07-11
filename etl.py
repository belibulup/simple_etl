import pandas as pd
from sqlalchemy import create_engine

# Extract : Read data from csv File
csv_file = 'data.csv'
data = pd.read_csv(csv_file)

# Transform: Clean and process the data
# Basic cleaning eg. drop rows with missing values
data.dropna(inplace=True)

# Converting string to integer for age / data type
data['age'] = data['age'].astype(int)

# Loading, save the data to SQLite database
db_file = "database.db"
engine = create_engine(f'sqlite:///{db_file}')
data.to_sql('people', engine, if_exists='replace', index=False)

print ("ETL process completed.")
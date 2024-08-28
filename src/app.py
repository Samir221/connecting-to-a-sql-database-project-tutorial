import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
# Retrieve individual database connection components from the environment variables
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Construct the database URL
database_url = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"

# Create the SQLAlchemy engine
engine = create_engine(database_url)


# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
try:
    # Read the SQL file
    with open('src/sql/create.sql', 'r') as file:
        sql_commands = file.read()

    # Connect to the database and execute the SQL commands
    with engine.connect() as connection:
        for sql_command in sql_commands.split(';'):
            sql_command = sql_command.strip()
            if sql_command:
                connection.execute(sql_command)
        print("Tables created successfully.")
except Exception as e:
    print("An error occurred while creating tables:", e)


# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

try:
    # Read the SQL file with insert commands
    with open('src/sql/insert.sql', 'r') as file:
        insert_commands = file.read()

    # Connect to the database and execute the insert commands
    with engine.connect() as connection:
        for insert_command in insert_commands.split(';'):
            insert_command = insert_command.strip()
            if insert_command:
                connection.execute(insert_command)
        print("Data inserted successfully.")
except Exception as e:
    print("An error occurred while inserting data:", e)


# 4) Use pandas to print one of the tables as dataframes using read_sql function
try:
    # Pass the engine directly to pandas.read_sql
    df = pd.read_sql("SELECT * FROM books", engine)

    # Print the DataFrame
    print(df)
except Exception as e:
    print("An error occurred while fetching data:", e)
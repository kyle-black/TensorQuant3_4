import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

dotenv_path = '../../.env'
load_dotenv(dotenv_path)

# Database connection parameters
db_config = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST'),
    "port": 16751,
    
    
    
}

create_tables =[
    '''
    CREATE TABLE IF NOT EXISTS EURUSD_15(
    date FLOAT PRIMARY KEY,
    open FLOAT,
    low FLOAT,
    high FLOAT,
    close FLOAT,
    volume FLOAT

    );
    '''

]

tablelist =['aliusd_15','audjpy_15', 'audusd_15','clusd_15', 'eurgbp_15','eurjpy_15', 
            'gbpjpy_15','gcusd_15','hgusd_15','gcusd_15','hgusd_15',
            'ngusd_15','nzdjpy_15','pausd_15','plusd_15', 'siusd_15', 
            'usdcad_15','usdchf_15','usdhkd_15','usdjpy_15']



try:
    # Establish the connection
    connection = psycopg2.connect(**db_config)
    print("Connection successful!")

    # Create a cursor object
    cursor = connection.cursor()


    for table in tablelist:

        command = f"""CREATE TABLE IF NOT EXISTS {table}(date FLOAT,
    open FLOAT,
    low FLOAT,
    high FLOAT,
    close FLOAT,
    volume FLOAT

    );
    """
        
#        command = f"DROP TABLE IF EXISTS {table}"
       # command =f"ALTER TABLE {table} DROP CONSTRAINT {table}_pkey;"
        cursor.execute(command)

    # Execute each table creation SQL command
  #  for command in create_tables:
  #       cursor.execute(command)
  #       print(f"Executed:\n{command}")
    
    # Commit the changes
    connection.commit()
 #   print("Table dropped successfully.")
    print("Tables created successfully.")

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed.")
except Exception as e:
    print("Error:", e)
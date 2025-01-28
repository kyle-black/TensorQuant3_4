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

tablelist =['aliusd_60','audjpy_60', 'audusd_60','clusd_60', 'eurgbp_60','eurjpy_60', 
            'gbpjpy_60','gcusd_60','hgusd_60','gcusd_60','hgusd_60',
            'ngusd_60','nzdjpy_60','pausd_60','plusd_60', 'siusd_60', 
            'usdcad_60','usdchf_60','usdhkd_60','usdjpy_60']

#tablelist= ['eurusd_15']

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
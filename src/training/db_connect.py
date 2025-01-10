import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os


load_dotenv()

# Database connection parameters
db_config = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST')
    "port": 16751,
    
    
    
}


try:
    # Establish the connection
    connection = psycopg2.connect(**db_config)
    print("Connection successful!")

    # Create a cursor object
   # cursor = connection.cursor()

    # Execute each table creation SQL command
   # for command in create_tables:
   #     cursor.execute(command)
   #     print(f"Executed:\n{command}")
    
    # Commit the changes
  #  connection.commit()
   # print("Tables created successfully.")

    # Close the cursor and connection
   # cursor.close()
    connection.close()
    print("Connection closed.")
except Exception as e:
    print("Error:", e)
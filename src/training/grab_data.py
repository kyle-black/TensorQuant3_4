from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create the database URL correctly
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Create the engine
engine = create_engine(DATABASE_URL)


def join_tables():
    # Write your raw SQL query
    sql_query = text("""
    SELECT 
        t1.date AS date,
        t1.open AS eurusd_open,
        t1.high AS eurusd_high,
        t1.low AS eurusd_low,
        t1.close AS eurusd_close,
        t1.volume AS eurusd_volume,
        t2.open AS eurjpy_open,
        t2.high AS eurjpy_high,
        t2.low AS eurjpy_low,
        t2.close AS eurjpy_close,
        t2.volume AS eurjpy_volume,
        t3.open AS eurgbp_open,
        t3.high AS eurgbp_high,
        t3.low AS eurgbp_low,
        t3.close AS eurgbp_close,
        t3.volume AS eurgbp_volume
    FROM eurusd_15 t1
    INNER JOIN eurjpy_15 t2 ON t1.date = t2.date
    INNER JOIN eurgbp_15 t3 ON t1.date = t3.date
    """)

    # Execute the query
    with engine.connect() as connection:
        result = connection.execute(sql_query)
        
        # Convert the results to a DataFrame
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Format the date
    df['formatted_time'] = pd.to_datetime(df['date'], unit='s')

    # Display the DataFrame
    print(df)
    return df


if __name__ == "__main__":
    print("Joining tables using raw SQL...")
    df = join_tables()
    print(df)
    # Save the result to a CSV file
    #df.to_csv("joined_data.csv", index=False)

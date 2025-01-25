from sqlalchemy import create_engine, Column, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# Database URL


# Create the engine
engine = create_engine(DATABASE_URL)

# Base class for models
Base = declarative_base()

# Define a model
class EURUSD(Base):
    __tablename__ = 'eurusd_15'
    date = Column(Float, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

def run_query():
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query the database
    results = (session.query(EURUSD).all())

    # Convert results to a DataFrame
    data = [
        {
            "DATE": bar.date,
            "OPEN": bar.open,
            "HIGH": bar.high,
            "LOW": bar.low,
            "CLOSE": bar.close,
            "VOLUME": bar.volume
        }
        for bar in results
    ]

    df = pd.DataFrame(data)
  #  df['formatted_time'] = df['DATE']/10000
    df['formatted_time'] = pd.to_datetime(df['DATE'], unit='ms')
    
    # Display the DataFrame
    print(df)
    return df





if __name__ == "__main__":
    run_query()

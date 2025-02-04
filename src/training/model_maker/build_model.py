import pandas as pd
import numpy as np
#import model  # Uncomment if you have a model module to import

class Data_Gather:
    def __init__(self, asset, threshold, trial, model_type, touch_barrier):
        self.asset = asset
        self.threshold = threshold
        self.trial = trial
        self.model_type = model_type
        self.touch_barrier = touch_barrier
    def grab_train_data(self):
        # Read in training data CSV
        self.train_data = pd.read_csv(f'../train_data/{self.asset}/{self.asset}_{self.threshold}_{self.trial}.csv')
        return self.train_data

    def grab_raw_data(self):
        self.raw_data = pd.read_csv(f'../train_data/{self.asset}/bars_15/{self.asset}_15.csv')
        return self.raw_data

    def create_end_barrier(self):
        """
        Create an 'end barrier' DataFrame by filtering raw_data to only include rows where the 'date' value 
        exists in train_data. For each matching row, get the next `touch_barrier` rows of raw_data and store 
        in a new column ('touches') a list of dictionaries where each dictionary contains both 'date' and 'eurusd_close'.
        """
        # Ensure that train_data and raw_data are loaded
        if not hasattr(self, 'train_data'):
            self.grab_train_data()
        if not hasattr(self, 'raw_data'):
            self.grab_raw_data()
            
        # Filter raw_data and train_data based on dates present in both
        in_raw = self.raw_data[self.raw_data['date'].isin(self.train_data['date'])]
        in_train = self.train_data[self.train_data['date'].isin(self.raw_data['date'])]
        
        # Create the new column 'touches' in train_data, defaulting to None
        self.train_data['touches'] = None

        # Loop over matching indices; assuming that in_raw and in_train are aligned in order
        for raw_idx, train_idx in zip(in_raw.index, in_train.index):
            # Get a slice of raw_data starting at raw_idx and spanning self.touch_barrier rows
            barrier = self.raw_data.iloc[raw_idx: raw_idx + self.touch_barrier]
            
            # Create a list of dictionaries with 'date' and 'eurusd_close'
            touches = barrier.apply(
                lambda row: {'date': row['date'], f'{self.asset}_close': row[f'{self.asset}_close']},
                axis=1
            ).tolist()
            
            # Use .at for single-cell assignment
            self.train_data.at[train_idx, 'touches'] = touches
           # print(f"Train row {train_idx}: {self.train_data.loc[train_idx]}")
        
        return self.train_data
    
    def label_data(self):
        
        self.train_data['pct_change'] =self.train_data[f'{self.asset}_close'].pct_change()
        self.train_data['rolling_pct_change_std'] = self.train_data['pct_change'].rolling(window=30).std()
        self.train_data['touch_barrier_upper'] = self.train_data[f'{self.asset}_close'] + (self.train_data[f'{self.asset}_close'] * self.train_data['rolling_pct_change_std'])
        self.train_data['touch_barrier_lower'] = self.train_data[f'{self.asset}_close'] - (self.train_data[f'{self.asset}_close'] * self.train_data['rolling_pct_change_std'])
        
        return self.train_data


class Model_Build(Data_Gather):
    pass

if __name__ == "__main__":
    run_ = Data_Gather('eurusd', 50000, 1, None, 5)
    train_df = run_.grab_train_data()
    raw_df = run_.grab_raw_data()
    barrier_df = run_.create_end_barrier()

    label_df =run_.label_data()
    
    print("Train Data:")
    print(train_df)
    print("\nRaw Data:")
    print(raw_df.head())
    print("\nEnd Barrier Data (rows in raw_data with dates in train_data):")
    print(barrier_df)
    print("\nLabel dataframe:")
    print(label_df)

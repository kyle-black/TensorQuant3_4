import pandas as pd
import grab_data


def bar_creation(df_, bar_threshold, asset_):

    bar_series = []
    high_series =[]
    low_series = []
    open_series =[]

    dollar_count =0 
    dollar_bar =[]

    for idx, bar in df_.iterrows():

        VOLUME_ = f'{asset_}_volume'
        HIGH_ = f'{asset_}_high'
        LOW_= f'{asset_}_low'
        CLOSE_ = f'{asset_}_close'
        OPEN_ =f'{asset_}_open'
        DATE_ = f'date'
        formatted_time_ = f'formatted_time'

        bar_count = (bar[VOLUME_])
        bar_series.append({idx:[bar, bar_count]})
        dollar_count += bar_count

       # if dollar_count == 0:
        #    open_  = bar.OPEN
            
            
        
        if dollar_count >= bar_threshold:
            
            high_series.append(bar[HIGH_])
            low_series.append(bar[LOW_])
            open_series.append(bar[OPEN_])

            open_ = open_series[0]
            print(open_series)
            high_ = max(high_series)
            low_ = min(low_series)
            close_ = bar[CLOSE_]
            date_ = bar[DATE_]
            formatted_time = bar[formatted_time_]

            dollar_bar.append({idx:[date_,open_, high_, low_, close_,formatted_time]})

            dollar_count = 0
            
            
            #### REsets series List after new bar is created
            open_series = []
            bar_series =[]
            high_series =[]
            low_sereies =[] 

        else:
            print('running dollar count', dollar_count )
            bar_series.append({idx:[bar[OPEN_], bar[HIGH_], bar[LOW_], bar[CLOSE_]]})
            
            open_series.append(bar[OPEN_])
            high_series.append(bar[HIGH_])
            low_series.append(bar[LOW_])

    #print(bar_series)
    return dollar_bar



if __name__ == "__main__":


    df  = grab_data.join_tables()
    threshold_ = 50000
    asset_= 'eurusd'

    print((bar_creation(df, threshold_,asset_)))
import pandas as pd
import grab_data


def bar_creation(df_, bar_threshold):

    bar_series = []
    high_series =[]
    low_series = []
    open_series =[]

    dollar_count =0 
    dollar_bar =[]

    for idx, bar in df_.iterrows():

        bar_count = (bar.VOLUME)
        bar_series.append({idx:[bar, bar_count]})
        dollar_count += bar_count

       # if dollar_count == 0:
        #    open_  = bar.OPEN
            
            
        
        if dollar_count >= bar_threshold:
            
            high_series.append(bar.HIGH)
            low_series.append(bar.LOW)
            open_series.append(bar.OPEN)

            open_ = open_series[0]
            print(open_series)
            high_ = max(high_series)
            low_ = min(low_series)
            close_ = bar.CLOSE
            date_ = bar.DATE
            formatted_time = bar.formatted_time

            dollar_bar.append({idx:[date_,open_, high_, low_, close_,formatted_time]})

            dollar_count = 0
            
            
            #### REsets series List after new bar is created
            open_series = []
            bar_series =[]
            high_series =[]
            low_sereies =[] 

        else:
            print('running dollar count', dollar_count )
            bar_series.append({idx:[bar.OPEN, bar.HIGH, bar.LOW, bar.CLOSE]})
            
            open_series.append(bar.OPEN)
            high_series.append(bar.HIGH)
            low_series.append(bar.LOW)

    #print(bar_series)
    return dollar_bar



if __name__ == "__main__":


    df  = grab_data.run_query()
    threshold_ = 5000

    print((bar_creation(df, threshold_)))


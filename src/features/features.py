import sys
import os
src_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(src_path)
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from data.DBServer import DBServer
from data import download_historical_data as dhd
from datetime import timedelta
from sklearn.model_selection import train_test_split, cross_val_score
from statsmodels.tsa.seasonal import STL
import xgboost as xgb
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
from statsmodels.graphics.tsaplots import plot_acf
import statsmodels.api as sm
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import TimeSeriesSplit
from copy import deepcopy

MAX_DAYS = 28

def read_data(db):
    dhd.db_validation(db)
    
    command = """SELECT e.effective_date,  e.bid, e.ask, c.code
    FROM exchange e
    JOIN currency c
        ON e.currency_id = c.ID
    WHERE c.code <> 'XDR'"""

    columns = ['date', 'bid', 'ask', 'code']
    df = pd.DataFrame(db.execute_command(command),columns=columns)
    df['date'] = pd.to_datetime(df['date'])
    df['rate'] = (df['bid'] + df['ask'])/2
    df = df[['date', 'code', 'rate']]
    return df
           
def get_features(df):
    df.loc[:,'year'] = df['date'].dt.year
    df.loc[:,'day_of_year'] = df['date'].dt.day_of_year
    df.loc[:,'month'] = df['date'].dt.month
    df.loc[:,'day'] = df['date'].dt.day
    df.loc[:,'day_of_week'] = df['date'].dt.weekday
    df.loc[:,'week_of_year'] = df['date'].dt.strftime("%W").apply(lambda x: int(x))
    df = df.reset_index()
    df = df.set_index('date')
    df = df.sort_values(by=['code', 'index'])
    stats = ['mean','min','max']
    roll_28 = df.groupby('code')['rate'].rolling(window=28,min_periods=1).agg(stats).shift(1).reset_index(level=0)
    roll_28.loc['2005-01-03',['mean','min','max']] = np.NaN
    df[['roll_mean_28','roll_min_28','roll_max_28']] = roll_28[['mean','min','max']]
    roll_90 = df.groupby('code')['rate'].rolling(window=90,min_periods=1).agg(stats).shift(1).reset_index(level=0)
    roll_90.loc['2005-01-03',['mean','min','max']] = np.NaN
    df[['roll_mean_90','roll_min_90','roll_max_90']] = roll_90[['mean','min','max']]
    df = df.set_index([df.index,'code'])
    df = df[~df.index.duplicated(keep='first')]
    df = df.reset_index(level=1)
    df['lag_28'] = df.groupby('code')['rate'].shift(28)
    del df['index']
    df = df.sort_index()
    df = df.set_index([df.index,'code'])
    return df





# def main():
#     try:
#         DAYS_TO_PREDICT = 14
#         server = DBServer(
#             server='DESKTOP-SFO3K5V',
#             user = 'user_1',
#             password = '9',
#             database = 'BI_project'
#         )
#         server.db_connect()
#         server.db_create_cursor()
        
#         df = read_data(server)
#         df = get_features(df)
        
#         model = train_model(df)
        
        

#     except Exception as e:
#         server.connection.rollback()
#         server.db_close_connection()
#         print(f"Error occurred: {e}")



# if __name__ == "__main__":
#     main()

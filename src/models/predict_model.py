import sys
import os
src_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(src_path)
import pandas as pd
import numpy as np
from data import download_historical_data as dhd
from datetime import timedelta

MAX_DAYS = 28

def get_max_date(db):
    dhd.db_validation(db)
    
    command = """SELECT max(e.effective_date) FROM exchange e"""
    max_date = db.execute_command(command)
    return max_date[0][0]

def get_currency_codes(db):
    dhd.db_validation(db)
    
    command = """SELECT DISTINCT c.code
        FROM exchange e
        JOIN currency c
            ON e.currency_id = c.ID
        WHERE c.code <> 'XDR'"""
    codes = db.execute_command(command)
    codes = [code[0] for code in codes]
    return codes

# MAX days to predict for the model: 28
def create_data_pred(db, days_to_predict):
    if days_to_predict > MAX_DAYS:
        raise Exception('Error in features.expand_data: days_to_predict parameter value exceeds 28 (max for the model).')

    max_date = get_max_date(db)
    codes = get_currency_codes(db)

    data = []
    for day in range(1,days_to_predict+1):
        date = max_date + timedelta(days=day)
        for code in codes:
            data.append((date,code,np.NaN))
    cols = ['date','code','rate']
    df = pd.DataFrame(data, columns=cols)
    df['date'] = pd.to_datetime(df['date'])
    return df

         
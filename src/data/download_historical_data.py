import requests
import json
import pandas as pd
import numpy as np
from .DBServer import DBServer
import time
from datetime import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def get_dates(start_year):
        start_dates = ['-01-01', '-04-01', '-07-01', '-10-01']
        end_dates = ['-03-31', '-06-30', '-09-30', '-12-31']
        years = [year for year in range(start_year,2025,1)]
        
        start_dates = [str(year)+date for year in years for date in start_dates]
        end_dates = [str(year)+date for year in years for date in end_dates]
        return (start_dates, end_dates)
    
    
def get_codes():
    url = f"http://api.nbp.pl/api/exchangerates/tables/C/today/"
    response = requests.get(url)     
    codes = [a['code'] for a in response.json()[0]['rates']]
    return codes
    
def get_response(code, start_date, end_date):
    url = f"http://api.nbp.pl/api/exchangerates/rates/C/{code}/{start_date}/{end_date}/?format=json"
    return requests.get(url)  

def get_currency_data(code,start_date, end_date):
    response = get_response(code, start_date, end_date)
    if response.status_code == 200:
        data = response.json()['rates']
        currency = response.json()['currency']
        df =  pd.DataFrame(data)
        df['currency'] = currency
        df['code'] = code
        return df
    else:
        return None

def create_df(code_list,start_dates,end_dates):
    df = None
    for i in range(len(start_dates)):
        for code in code_list:
            df_one = get_currency_data(code,start_dates[i],end_dates[i])
            if type(df_one) == pd.DataFrame:
                print(df_one.shape)
                if type(df) != pd.DataFrame:
                    df =  df_one     
                else:
                    df = pd.concat([df,df_one],axis=0) 
    
    max_data = max(df['effectiveDate'])
    today = datetime.today().strftime('%Y-%m-%d')
    for code in code_list:
        df_one = get_currency_data(code,max_data,today)
        if type(df_one) == pd.DataFrame:
            if type(df) != pd.DataFrame:
                df =  df_one     
            else:
                df = pd.concat([df,df_one],axis=0) 
    return df   


def denormalize_data(data):
    
    data['currency'] = data['currency'].replace({
        'forint (Węgry)':'forint węgierski',
        'jen (Japonia)':'jen japoński'
    })
    
    table_currency = pd.DataFrame(data={
        'code': data['code'].unique(),
        'currency': data['currency'].unique()     
    }) 
    table_currency = table_currency.sort_values(by='code').reset_index(drop=True)
    table_currency['id'] = table_currency.index + 1
    
    table_rates = data.merge(table_currency, on='code')
    table_rates['currency_id'] = table_rates['id']
    table_rates = table_rates[['effectiveDate','no','bid','ask','currency_id']]
    del table_currency['id']
    return (table_currency,table_rates)

def db_validation(db):
    if not isinstance(db, DBServer):
        raise Exception('Provided db must be of class DBServer')
    if db.connection == None or db.cursor == None:
        raise Exception('Connection and cursor for DBServer object must be initialized')

def insert_currency(db,df):
    db_validation(db)
    
    insert_statement = "INSERT INTO currency values  "
    df.columns =  range(df.shape[1])
    for i, row in df.iterrows():
        if i == df.shape[0] - 1:
            insert_statement += f"('{row[0]}','{row[1]}')"
        else: 
            insert_statement += f"('{row[0]}','{row[1]}'), "
        
    db.execute_command(insert_statement)    
    
def insert_exchange(db,df):
    db_validation(db)
    
    insert_statement = "INSERT INTO exchange values  "
    df.columns =  range(df.shape[1])
    for i, row in df.iterrows():
        if i == df.shape[0] - 1:
            insert_statement += f"('{row[0]}','{row[1]}',{row[2]},{row[3]},{row[4]})"
        else:
            insert_statement += f"('{row[0]}','{row[1]}',{row[2]},{row[3]},{row[4]}), "
    db.execute_command(insert_statement)   

def split_df_in_chunks(df):
    df = df.sort_values(by='effectiveDate')
    chunks = [chunk for chunk in range(1000,df.shape[0],1000)]
    chunks.append(df.shape[0])
    df_chunks = []
    for i in range(len(chunks)):
        if i == 0:
            df_chunks.append(df.iloc[0:chunks[i],:])
        else:
            df_chunks.append(df.iloc[chunks[i-1]:chunks[i],:])
    return df_chunks        

def get_currency_count(db):
    db_validation(db)
    command = """SELECT COUNT(*) FROM currency c"""
    return db.execute_command(command)[0][0]
    
def insert_data(db,df_chunks):
    db_validation(db)
    for df in df_chunks:
        currency, exchange = denormalize_data(df) 
        if get_currency_count(db) == 0:
            insert_currency(db,currency)
        insert_exchange(db,exchange)  
        
                
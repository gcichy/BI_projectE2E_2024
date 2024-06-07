from . import download_historical_data as dhd
from .DBServer import DBServer  
import sys
import os
src_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.append(src_path)
from features import features as f
import datetime
import pandas as pd


def create_update_df(code_list):
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    df = None
    for code in code_list:
        df_one = dhd.get_currency_data(code,today,today)
        if type(df_one) == pd.DataFrame:
            print(df_one.shape)
            if type(df) != pd.DataFrame:
                df =  df_one     
            else:
                df = pd.concat([df,df_one],axis=0) 
    return df

def get_currency_df(db):
    dhd.db_validation(db)
    
    command = """SELECT DISTINCT c.code, c.id
        FROM currency c
        WHERE c.code <> 'XDR'"""
    codes_df = db.execute_command(command)
    codes_df  = pd.DataFrame(codes_df, columns=['code','currency_id'])
    return codes_df

def prepare_pred(db,df):
    df = df[['rate_pred']].reset_index()
    
    codes_df = get_currency_df(db)
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    
    df['created_on'] = today
    df = df.merge(codes_df,on='code')
    del df['code']    
    return df

def insert_prediction(db,df):
    dhd.db_validation(db)
    
    insert_statement = "INSERT INTO prediction values  "
    df.columns =  range(df.shape[1])
    for i, row in df.iterrows():
        if i == df.shape[0] - 1:
            insert_statement += f"('{row[0]}',{row[1]},'{row[2]}',{row[3]})"
        else:
            insert_statement += f"('{row[0]}',{row[1]},'{row[2]}',{row[3]}), "
    db.execute_command(insert_statement)  

def concat_dfs(df,pred_df):
    min_date = pred_df['date'].min()
    pred_df = pd.concat([df,pred_df],ignore_index=True)
    pred_df = f.get_features(pred_df)
    pred_df = pred_df[pred_df.index.get_level_values('date') >= min_date]
    del pred_df['rate']
    return pred_df

def main():
    try:
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        code_list = dhd.get_codes()
        df = create_update_df(code_list, today)
    
        print(df)
        
        server = DBServer(
            server='DESKTOP-SFO3K5V',
            user = 'user_1',
            password = '9',
            database = 'BI_project'
        )
        server.db_connect()
        server.db_create_cursor()
        print('Connected to the server.')

        
        dhd.insert_data(server,df)
        print('Data inserted.')
        
        server.connection.commit()
        server.db_close_connection()
           
    except Exception as e:
        server.connection.rollback()
        server.db_close_connection()
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
    

    
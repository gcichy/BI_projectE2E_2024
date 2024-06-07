import os
from data.DBServer import DBServer
from features import features as f
from models import predict_model as pm
import datetime
import data.download_historical_data as dhd
import data.update_data as ud

import joblib
import pandas as pd

DAYS_TO_PREDICT = 28
     
def main():
    try:
        server = DBServer(
            server='DESKTOP-SFO3K5V',
            user = 'user_1',
            password = '9',
            database = 'BI_project'
        )
        server.db_connect()
        server.db_create_cursor()
        
        
        model = joblib.load(os.path.join(os.path.dirname(__file__),'models','forex_xgb_model.pkl'))
        
        df = f.read_data(server)
        pred_df = pm.create_data_pred(server, DAYS_TO_PREDICT)
        pred_df = ud.concat_dfs(df,pred_df)
        
        pred_df['rate_pred'] = model.predict(pred_df)

        pred_df = ud.prepare_pred(server,pred_df)
        
        ud.insert_prediction(server,pred_df)
        
        server.connection.commit()
        server.db_close_connection()
        
    except Exception as e:
        server.connection.rollback()
        server.db_close_connection()
        print(f"Error occurred: {e}")



if __name__ == "__main__":
    main()
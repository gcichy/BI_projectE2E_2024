import os
from data.DBServer import DBServer
from features import features as f
from models import predict_model as pm
import datetime
import data.download_historical_data as dhd
import data.update_data as ud
import models.train_model as tm

import joblib
import pandas as pd
from make_prediction import DAYS_TO_PREDICT
     
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
        
        
        codes = dhd.get_codes()[:-1]
        upd_df = ud.create_update_df(codes)
        
        dhd.insert_data(server,[upd_df])
        print('data inserted')
        
        df = f.read_data(server)
        df = f.get_features(df)
        
        model = tm.train_model(df)
        print('model trained')
        
        joblib.dump(model, os.path.join(os.path.dirname(__file__),'models','forex_xgb_model.pkl'))
        print('model updated')
        
        pred_df = pm.create_data_pred(server, DAYS_TO_PREDICT)
        pred_df = ud.concat_dfs(df.reset_index()[['date','code','rate']],pred_df)
        
        
        pred_df['rate_pred'] = model.predict(pred_df)
        print('prediction created')

        pred_df = ud.prepare_pred(server,pred_df)
 
        ud.insert_prediction(server,pred_df)
        print('prediction inserted')
        
        server.connection.commit()
        server.db_close_connection()
        
    except Exception as e:
        server.connection.rollback()
        server.db_close_connection()
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
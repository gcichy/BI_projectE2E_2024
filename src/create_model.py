import os
from data.DBServer import DBServer
from features import features as f
from models import train_model as tm
import joblib

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
        
        df = f.read_data(server)
        df = f.get_features(df)
        
        model = tm.train_model(df)
       
        joblib.dump(model, os.path.join(os.path.dirname(__file__),'models','forex_xgb_model.pkl'))

    except Exception as e:
        server.connection.rollback()
        server.db_close_connection()
        print(f"Error occurred: {e}")



if __name__ == "__main__":
    main()
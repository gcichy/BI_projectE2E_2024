from data import download_historical_data as dhd
from data.DBServer import DBServer  

def main():
    try:
        START_YEAR = 2005
        
        server = DBServer(
            server='DESKTOP-SFO3K5V',
            user = 'user_1',
            password = '9',
            database = 'BI_project'
        )
        server.db_connect()
        server.db_create_cursor()
        print('Connected to the server.')
        server.cursor.callproc('AP_recreate_tables')
        
        start_dates, end_dates = dhd.get_dates(START_YEAR)
        code_list = dhd.get_codes()
        
        print("Before data download.")
        df = dhd.create_df(code_list,start_dates, end_dates)
        df
        print("DataFrame created.")
        
        df_chunks = dhd.split_df_in_chunks(df)
        print('DataFrame splitted.')
  
        dhd.insert_data(server,df_chunks)
        print('Data inserted.')
        
        server.connection.commit()
        server.db_close_connection()
           
    except Exception as e:
        server.connection.rollback()
        server.db_close_connection()
        print(f"Error occurred: {e}")



if __name__ == "__main__":
    main()
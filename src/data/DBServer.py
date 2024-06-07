import pandas as pd
import numpy as np
import pymssql

class DBServer:  
    def __init__(self,server, user, password, database):       
        self.server_name = server
        self.user_name = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
    def db_connect(self):
        try:
            
            conn = pymssql.connect(
                server = self.server_name,
                user = self.user_name,
                password = self.password,
                database = self.database
            )
            self.connection = conn
        except Exception as e:
            raise Exception(f'DBServer.db_connect method: Failed to connect to the database: {e}')
    
    def db_create_cursor(self):
        if not isinstance(self.connection,pymssql._pymssql.Connection):
            raise Exception(f"DBServer.db_create_cursor method: connection is not established properly - self.connection must be of class 'pymssql._pymssql.Connection'")   
        else:
            try:
                self.cursor = self.connection.cursor()
            except Exception as e:
                raise Exception(f"DBServer.db_create_cursor method: failed to create cursor: {e}")

    def execute_command(self,command):
        if self.connection == None or self.cursor == None:
            raise Exception('DBServer.execute_command method: Connection and cursor for DBServer object must be initialized')
        try:
            self.cursor.execute(command)
            rows = self.cursor.fetchall() if self.cursor.rowcount == -1 else None
            
            return rows
        
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"DBServer.execute_command method: failed to execute command: {e}")
        
    def db_close_connection(self):
        self.connection.close()    
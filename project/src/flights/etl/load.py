import pandas as pd
from database.postgres import PostgresDB
import os
import requests
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
from flights.etl.extract import Extract

class Load():

    @staticmethod
    def load(df:pd.DataFrame,engine)->None:
        '''
        this function will get data for today airplane and then upsert to the database
        '''
        meta = MetaData()
        flights_table = Table(
            "flight_data", meta,
            Column("pull_date", String, primary_key=True),
            Column("airport_code", String, primary_key=True),
            Column("departures_onTime", Integer),
            Column("departures_delayed", Integer),
            Column("departures_canceled", Integer),
            Column("arrivals_onTime", Integer),
            Column("arrivals_delayed", Integer),
            Column("arrivals_canceled", Integer)
        )
        
        # gather only the history api-data-files
        # all_files = os.listdir()    
        # csv_files = list(filter(lambda f: f.startswith('api_extract_') and f.endswith('.csv'), all_files))
        # # initialize list of dates represented by csv-files
        # history_date_list = [] 
        # for i in range(len(csv_files)):
        #     history_date_list.append(dt.datetime.strptime(csv_files[i][12:22], "%Y-%m-%d"))
        # # sorting dates descending order (highest value first)
        # history_date_list.sort(reverse=True)
        
        # define ORM data types
        meta.create_all(engine) # creates table if it does not exist
        
        # Compare the log-file ("log_history.csv") of processing the latest history files:
        # In case the latest csv-file matches the date of the log-file all history data are 
        # already loaded in the database. 
        # In case the dates of both don't match all available csv-files will be loaded 
        # into the database together.
        # if log_history == history_date_list[0]:
        #     print("All history csv-files are already in the data base. Proceeding with the API results of the current request!")    
        
        insert_statement = postgresql.insert(flights_table).values(df.to_dict(orient='records'))
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=['pull_date', 'airport_code'],
            set_={c.key: c for c in insert_statement.excluded if c.key not in ['pull_date', 'airport_code']})
        engine.execute(upsert_statement)



if __name__=='__main__':
    engine = PostgresDB.create_pg_engine()
    # df = Extract() --read from gernot
    # df = pd.read_csv('data/api_extract_2023-02-06.csv') #  api for today
    df = Extract.extract_airport_list("data/airports.csv")
    Load.load(df,engine=engine)
    # transform

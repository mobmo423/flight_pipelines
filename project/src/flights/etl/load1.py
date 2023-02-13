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

class Load():

    @staticmethod
    def load(df:pd.DataFrame,engine)->None:
        '''
        this function will get data for today airplane and then upsert to the database
        '''
        meta = MetaData()
        flights_table = Table(
            "flights", meta,
            Column("pull_date", String, primary_key=True),
            Column("airport_code", String, primary_key=True),
            Column("departures_onTime", Integer),
            Column("departures_delayed", Integer),
            Column("departures_canceled", Integer),
            Column("arrivals_onTime", Integer),
            Column("arrivals_delayed", Integer),
            Column("arrivals_canceled", Integer)
        )
        meta.create_all(engine) # creates table if it does not exist
        insert_statement = postgresql.insert(flights_table).values(df.to_dict(orient='records'))
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=['pull_date', 'airport_code'],
            set_={c.key: c for c in insert_statement.excluded if c.key not in ['pull_date', 'airport_code']})
        engine.execute(upsert_statement)



if __name__=='__main__':
    engine = PostgresDB.create_pg_engine()
    # df = Extract() --read from gernot
    df = pd.read_csv('data/api_extract_2023-02-06.csv')
    Load.load(df,engine=engine)
    # transform

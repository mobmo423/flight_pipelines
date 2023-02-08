from sqlalchemy import Integer, String, Float, JSON , DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON 
import jinja2 as j2 
import pandas as pd
import numpy as np
import logging 
from sqlalchemy.dialects import postgresql

class Load():
    @staticmethod
    def overwrite_to_database(df: pd.DataFrame, table_name: str, engine)->bool: 
        """
        Upsert dataframe to a database table 
        - `df`: pandas dataframe 
        - `table`: name of the target table 
        - `engine`: connection engine to database 
        """
        logging.info(f"Writing to table: {table_name}")
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        logging.info(f"Successful write to table: {table_name}, rows inserted/updated: {len(df)}")
        return True 
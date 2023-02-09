import pandas as pd
import jinja2 as j2 
import logging 
import os 
import datetime as dt 
from database.postgres import PostgresDB
from sqlalchemy import Integer, String, Float, JSON , DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON 

class Extract():

    @staticmethod
    def extract_from_database(table_name, engine, path)->pd.DataFrame:
        logging.info(f"Extracting table: {table_name}")
        if f"{table_name}.sql" in os.listdir(path):
            # read sql contents into a variable 
            with open(f"{path}/{table_name}.sql") as f: 
                raw_sql = f.read()

            #config = j2.Template(raw_sql).make_module().config 
            parsed_sql = j2.Template(raw_sql).render(source_table = table_name, engine=engine)
            df = pd.read_sql(sql=parsed_sql, con=engine)
            logging.info(f"Successfully extracted table: {table_name}, rows extracted: {len(df)}")
            return df 
        else:
            logging.error(f"Could not find table: {table_name}")


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

class Transform:
    def __init__(self,table_name,engine,models_path) -> None:
        '''
        The constructor will set us the basic requirements to perform the transform
        - table_name: table name as present in the model
        - engine: connection to our database
        - models_path: folder path where our model lives
        '''
        self.table_name = table_name
        self.engine = engine
        self.models_path = models_path
    
    def run(self)->bool:
        '''
        This function will read raw sql and perform transformation
        '''
        logging.basicConfig(level=logging.INFO)
        if f'{self.table_name}.sql' in os.listdir(self.models_path):

            logging.info(f'Starting to build model {self.table_name}')                                                                                                    
            with open(f"{self.models_path}/{self.table_name}.sql") as f:
                raw_sql = f.read()
            parsed_sql = j2.Template(raw_sql).render(target_table = self.table_name, engine=self.engine)
            result = self.engine.execute(parsed_sql)
            logging.info(f'Successfully built model {self.table_name}, rows: {result.rowcount}')
            return True
        else:
            logging.error(f'could not find {self.table_name} in {self.models_path}')
import pandas as pd
import jinja2 as j2 
import logging 
import os 
import datetime as dt 
from database.postgres import PostgresDB
from sqlalchemy import Integer, String, Float, JSON , DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON 



def get_incremental_value(table_name, path="extract_log"):
    df = pd.read_csv(f"{path}/{table_name}.csv")
    return df[df["log_date"] == df["log_date"].max()]["incremental_value"].values[0]

def upsert_incremental_log(log_path, table_name, incremental_value)->bool:
    if f"{table_name}.csv" in os.listdir(log_path):
        df_existing_incremental_log = pd.read_csv(f"{log_path}/{table_name}.csv")
        df_incremental_log = pd.DataFrame(data={
            "log_date": [dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")], 
            "incremental_value": [incremental_value]
        })
        df_updated_incremental_log = pd.concat([df_existing_incremental_log,df_incremental_log])
        df_updated_incremental_log.to_csv(f"{log_path}/{table_name}.csv", index=False)
    else: 
        df_incremental_log = pd.DataFrame(data={
            "log_date": [dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")], 
            "incremental_value": [incremental_value]
        })
        df_incremental_log.to_csv(f"{log_path}/{table_name}.csv", index=False)
    return True 


class Extract():

    @staticmethod
    def extract_from_database(table_name, engine, path)->pd.DataFrame:
        logging.info(f"Extracting table: {table_name}")
        if f"{table_name}.sql" in os.listdir(path):
            # read sql contents into a variable 
            with open(f"{path}/{table_name}.sql") as f: 
                raw_sql = f.read()
            # read the config
            config = j2.Template(raw_sql).make_module().config 

            if config['extract_type'].lower() == 'incremental':
                incremental_path = 'extract_log'
                if not os.path.exists(incremental_path):
                    os.mkdir(incremental_path)
                # check if there's a csv file
                if f'{table_name}.csv' in os.listdir(incremental_path):
                    current_max_incremental_value = get_incremental_value(table_name, path=incremental_path)
                    parsed_sql = j2.Template(raw_sql).render(source_table = table_name, engine=engine, is_incremental=True, incremental_value=current_max_incremental_value)
                    # execute incremental extract
                    df = pd.read_sql(sql=parsed_sql, con=engine)
                    # update max incremental value 
                    if len(df) > 0: 
                        max_incremental_value = df[config["incremental_column"]].max()
                    else: 
                        max_incremental_value = current_max_incremental_value
                    upsert_incremental_log(log_path=incremental_path, table_name=table_name, incremental_value=max_incremental_value)
                    logging.info(f"Successfully extracted table: {table_name}, rows extracted: {len(df)}")
                    return df 
                else: 
                    # it's the first time we run this incremental extract model
                    parsed_sql = j2.Template(raw_sql).render(source_table = table_name, engine=engine)
                    # perform full extract 
                    df = pd.read_sql(sql=parsed_sql, con=engine)
                    # store latest incremental value 
                    max_incremental_value = df[config["incremental_column"]].max()
                    upsert_incremental_log(log_path=incremental_path, table_name=table_name, incremental_value=max_incremental_value)
                    logging.info(f"Successfully extracted table: {table_name}, rows extracted: {len(df)}")
                    return df 

            else: 
                # it's a full load 
                parsed_sql = j2.Template(raw_sql).render(source_table = table_name, engine=engine)
                # perform full extract 
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
        if df is None:
            logging.info(f"there's nothing we could write to table: {table_name}")
            return False
        else:
            # append
            #df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            logging.info(f"Successful write to table: {table_name}, rows inserted/updated: {len(df)}")
            return True 

class Transform:
    def __init__(self,table_name,engine,models_path) -> object:
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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    source_engine = PostgresDB.create_pg_engine()
    target_engine = PostgresDB.create_pg_engine(kind='target')
    df = Extract.extract_from_database(table_name='flight_data',engine = source_engine,path='flights/models/extract/')
    Load.overwrite_to_database(df,table_name='flight_data',engine = target_engine)
    transform = Transform(table_name='staging_flights',engine=target_engine,models_path='flights/models/transform/')
    transform.run()
from database.postgres import PostgresDB
import pandas as pd
import os
import logging 
import jinja2 as j2 

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
            logging.info(f'Successfully built modekl {self.table_name}, rows: {result.rowcount} ')
            return True
        else:
            logging.error(f'could not find {self.table_name} in {self.models_path}')

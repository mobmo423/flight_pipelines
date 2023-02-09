import pandas as pd
import logging

class TestLoad():
    
    def __init__(self,date,engine,table_name,num_record=3)->object:
        self.date = date
        self.engine = engine
        self.table_name = table_name
        self.num_record = num_record

    def run(self)->bool:
        '''
        connect to the database
        function that takes the date, engine, num_record = 3
        check how many record were added  on a specific
        '''
        logging.info('starting data quality test')
        df = pd.read_sql(f"SELECT * FROM {self.table_name} where pull_date = '{self.date}'",self.engine)
        assert len(df) == self.num_record, f"actual rows: {len(df)}, expected_rows: {self.num_record} "
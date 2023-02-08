import pandas as pd
import jinja2 as j2 
import logging 
import os 
import datetime as dt 


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
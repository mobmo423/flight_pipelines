from flights.pipeline.transform_pipeline import run_pipeline
from flights.etl.extract import Extract
from flights.etl.load1 import Load
from database.postgres import PostgresDB

def run():
    engine = PostgresDB.create_pg_engine()
    # df = Extract() --read from gernot
    # df = pd.read_csv('data/api_extract_2023-02-06.csv') #  api for today
    df = Extract.extract_airport_list("data/airports.csv")
    Load.load(df,engine=engine)
    run_pipeline()

if __name__=='__main__':
    run()
    print('success')
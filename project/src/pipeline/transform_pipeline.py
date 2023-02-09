from database.postgres import PostgresDB
from pipeline.extract_load_pipeline import ExtractLoad
from flights.etl.transform import Transform
import logging
from data_quality_test.test_load import TestLoad 
from graphlib import TopologicalSorter


def run_pipeline():
    logging.basicConfig(level=logging.INFO)
    
    path_extract_model = 'models/extract/'
    path_transform_model = 'models/transform/'

    logging.info('connecting to sources and target databases')
    source_engine = PostgresDB.create_pg_engine(kind='source')
    logging.info('successfully connected to source databases')
    target_engine = PostgresDB.create_pg_engine(kind='target')
    logging.info('successfully connecting to target databases')

    dag = TopologicalSorter()
    logging.info("Creating extract and load nodes")
    node_extract_load = ExtractLoad(source_engine=source_engine,target_engine=target_engine,table_name='flight_data',path=path_extract_model)
    dag.add(node_extract_load)
    
    data_quality_test = TestLoad(date = '2023-02-06',table_name='flight_data',engine = source_engine,num_record=4)
    dag.add(data_quality_test,node_extract_load)
    logging.info("Creating Transform and load nodes")
    node_staging_flight = Transform(table_name='staging_flights',engine=target_engine,models_path=path_transform_model)
    #node_serving_films_popular = Transform(...)

    dag.add(node_staging_flight,node_extract_load)
    # dag.add(...,node_extract_load)
    # dag.add(...,node_extract_load)
    
    dag_rendered = tuple(dag.static_order())
    logging.info("Executing DAG")
    for node in dag_rendered: 
        node.run()

    logging.info("Pipeline run successful")


 
if __name__ == "__main__":
    run_pipeline()
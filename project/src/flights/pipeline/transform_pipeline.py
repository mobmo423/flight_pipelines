from database.postgres import PostgresDB
from flights.pipeline.extract_load_pipeline import ExtractLoad
from flights.etl.transform import Transform
import logging
import os
import yaml
from data_quality_test.test_load import TestLoad 
from graphlib import TopologicalSorter


def run_pipeline():
    logging.basicConfig(level=logging.INFO)
    
    with open("flights/config.yaml") as stream:
        config = yaml.safe_load(stream)
    path_extract_model = config['transform']['path_extract_model']
    path_transform_model = config['transform']['path_transform_model']

    logging.info('connecting to sources and target databases')
    source_engine = PostgresDB.create_pg_engine(kind='source')
    logging.info('successfully connected to source databases')
    target_engine = PostgresDB.create_pg_engine(kind='target')
    logging.info('successfully connecting to target databases')

    dag = TopologicalSorter()
    nodes_extract_load = []
    logging.info("Creating extract and load nodes")
    for table_name in os.listdir(path_extract_model):
        table_name = table_name.replace('.sql','')
        node_extract_load = ExtractLoad(source_engine=source_engine,target_engine=target_engine,table_name=table_name,path=path_extract_model)
        nodes_extract_load.append(node_extract_load)
        dag.add(node_extract_load)
    
    # replace date by today date 
    data_quality_test = TestLoad(date = '2023-02-06',table_name=config['transform']['raw_table'],engine = source_engine,num_record=config['data_quality_test']['num_record']) # number of airport you expect your api to retreive for a particular day 
    dag.add(data_quality_test,node_extract_load)
    logging.info("Creating Transform and load nodes")

    # add config
    node_staging_flight = Transform(table_name=config['transform']['staging_table'],engine=target_engine,models_path=path_transform_model)
    #node_serving_films_popular = Transform(...)

    dag.add(node_staging_flight,*nodes_extract_load)
    # dag.add(...,node_extract_load)
    # dag.add(...,node_extract_load)
    
    dag_rendered = tuple(dag.static_order())
    logging.info("Executing DAG")
    for node in dag_rendered: 
        node.run()

    logging.info("Pipeline run successful")


 
if __name__ == "__main__":
    run_pipeline()
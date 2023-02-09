from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os 

class PostgresDB():

    @staticmethod
    def create_pg_engine(kind = 'source'):
        """
        create an engine to either `source` or `target`
        """
        if kind == 'source':
            db_user = os.environ.get("source_db_user")
            db_password = os.environ.get("source_db_password")
            db_server_name = os.environ.get("source_db_server_name")
            db_database_name = os.environ.get("source_db_database_name")
        elif kind =='target':
            db_user = os.environ.get("target_db_user")
            db_password = os.environ.get("target_db_password")
            db_server_name = os.environ.get("target_db_server_name")
            db_database_name = os.environ.get("target_db_database_name")

        # create connection to database 
        connection_url = URL.create(
            drivername = "postgresql+pg8000", 
            username = db_user,
            password = db_password,
            host = db_server_name, 
            port = 5432,
            database = db_database_name, 
        )
        engine = create_engine(connection_url)
        return engine 
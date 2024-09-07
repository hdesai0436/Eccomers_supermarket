import psycopg2
import pandas as pd
import os
from app_log.logger import setup_logger
class PostgreSQLExtractor:
    def __init__(self) -> None:
        self.logger = setup_logger(__file__)

        

    def create_connection(self):
        # self.logger.debug('making connection to postgresql database')
        self.logger.info('making connection to postgresql database')
        try:
            conn = psycopg2.connect(
                dbname='eccomers',
                user='postgres',
                password='desai123',
                host='localhost',
                port='5432'
            )
            self.logger.info('successfully make connection with database')
            return conn
        except Exception as e:
            self.logger.error('something went wrong while making connection database {e}')
            raise e
        

    def extract_table_to_csv(self,conn,table_name,output_dir):
        try:
            self.logger.info(f'reading data from table {table_name}')
            df = pd.read_sql(f'select * from {table_name}',conn)
            csv_file_name = os.path.join(output_dir,f'{table_name}.csv')
            df.to_csv(csv_file_name, index=False)
            self.logger.info(f'successfully store raw data locally for table {table_name} and path is {csv_file_name}')
        except Exception as e:
            self.logger.error(f'create error while extracting data from the database and table name is: {table_name}')
            raise e
        

    def extract_mutiple_tables(self,conn,tables,output_dir):
        try:
            
            os.makedirs(output_dir, exist_ok=True)
            
            for table in tables:
                self.extract_table_to_csv(conn,table,output_dir)

        except Exception as e:
            raise e


    




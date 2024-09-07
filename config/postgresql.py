import psycopg2
import pandas as pd
import os

class PostgreSQLExtractor:
    def __init__(self) -> None:
        pass

    def create_connection(self):
        try:
            conn = psycopg2.connect(
                dbname='eccomers',
                user='postgres',
                password='desai123',
                host='localhost',
                port='5432'
            )
            return conn
        except Exception as e:
            raise e
        

    def extract_table_to_csv(self,conn,table_name,output_dir):
        try:
            df = pd.read_sql(f'select * from {table_name}',conn)
            csv_file_name = os.path.join(output_dir,f'{table_name}.csv')
            df.to_csv(csv_file_name, index=False)
        except Exception as e:
            raise e
        

    def extract_mutiple_tables(self,conn,tables,output_dir):
        try:
            
            os.makedirs(output_dir, exist_ok=True)
            
            for table in tables:
                self.extract_table_to_csv(conn,table,output_dir)

        except Exception as e:
            raise e


    




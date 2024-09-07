from config.postgresql import PostgreSQLExtractor
from config.google_storage import Google_storage
import os


class DataExtractor:
    def __init__(self,path,tables):
        self.path = path
        self.tables = tables
        self.postgre = PostgreSQLExtractor()
        self.google = Google_storage()
        self.conn = self.postgre.create_connection()
        self.output_dir = os.path.join(self.path,'data','raw')

    def extract_postgre_data(self):
        if self.conn:
             self.postgre.extract_mutiple_tables(self.conn,self.tables,self.output_dir)
             self.conn.close()

    def upload_gcp(self):
        self.google.upload(self.tables)




    



      
import datetime
from google.cloud import storage
from dotenv import load_dotenv
import pandas as pd
import os
os.environ['key.json'] = 'C:/Users/hardi/Documents/Eccomers/config/key.json'
creds  = os.getenv('key.json')

class Google_storage:
    def __init__(self,bucket_name='eccomer_supermarket',key_path= creds):
        self.bucket_name = bucket_name
        self.key_path = key_path
        self.path ="C:/Users/hardi/Documents/Eccomers"
        

    def get_client(self):
        try:
            storage_client = storage.Client.from_service_account_json(self.key_path)
            return storage_client
        except Exception as e:
            
            raise e
        
    def create_folder(self,table_name):
        try:
            
            year = datetime.datetime.now().year
            date = datetime.datetime.now().strftime("%Y-%m-%d")
            
            folder_path = f"raw/{year}/{date}/{table_name}/{table_name}.csv"
            blob_path = folder_path 
            return blob_path
        except Exception as e:
            raise e
        
    def upload(self,tables):
        
        client = self.get_client()
        bucket = client.bucket(self.bucket_name)
        for i in tables:
            csv_filename = os.path.join(f'{self.path}/data/raw/{i}.csv')
            gcp_path = self.create_folder(i)
            blob = bucket.blob(gcp_path)
            blob.upload_from_filename(csv_filename)
            
                

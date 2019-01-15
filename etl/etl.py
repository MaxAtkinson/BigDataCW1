from abc import ABC, abstractmethod
from db.mysql import cursor
from etl.mongo import MongoTL
from etl.neo import NeoTL

class ETL():
    def extract(self):
        cursor.execute('SELECT * FROM Album')
        for x in cursor:
            print(x)
        
    def run(self):
        self.extract()
        jobs = [MongoTL(), NeoTL()]
        for job in jobs:
            job.run()

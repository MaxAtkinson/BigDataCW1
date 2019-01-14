from abc import ABC, abstractmethod
from db.mysql import cursor

class ETL(ABC):
    def extract(self):
        cursor.execute('SELECT * FROM Album')
        for x in cursor:
            print(x)


    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass
        
    def run(self):
        self.extract()
        self.transform()
        self.load()

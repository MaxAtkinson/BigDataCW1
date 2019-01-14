from abc import ABC, abstractmethod
from db.mysql import cursor

class ETL(ABC):
    def extract(self):
        cursor.execute('SELECT * FROM test1')
        cursor.fetchall()
        print(cursor.rowcount)
        for x in cursor:
            print(x)

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass
        
    def run(self):
        pass

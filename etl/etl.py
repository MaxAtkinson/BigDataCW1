from abc import ABC, abstractmethod
from db.mysql import cursor

cursor.execute('SELECT * FROM Album')
cursor.fetchall()
print(cursor.rowcount)

class ETL(ABC):
    def extract(self):
        cursor.execute('SELECT * FROM album')
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

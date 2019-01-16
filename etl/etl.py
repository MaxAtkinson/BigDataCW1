import csv
from abc import ABC, abstractmethod
from db.mysql import cursor

class ETL(ABC):
    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def queries(self):
        pass

    def run(self):
        self.extract()
        self.transform()
        self.load()
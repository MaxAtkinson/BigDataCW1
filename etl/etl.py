from abc import ABC, abstractmethod

class ETL(ABC):
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

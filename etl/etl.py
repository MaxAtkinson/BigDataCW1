from abc import ABC, abstractmethod

class ETL(ABC):
    def extract(self):
        pass

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def load(self):
        pass

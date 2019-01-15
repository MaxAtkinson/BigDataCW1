from abc import ABC, abstractmethod

class TransformLoadTask(ABC):
    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass
        
    def run(self):
        self.transform()
        self.load()
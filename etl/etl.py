import csv
from abc import ABC, abstractmethod

class ETL(ABC):
    def write_query_to_file(self, cursor, entity_name):
        with open(f'data/mongo/{entity_name}.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(cursor.column_names)
            for row in cursor:
                writer.writerow(row)

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
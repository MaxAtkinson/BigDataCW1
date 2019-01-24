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
    def transform_and_load(self):
        pass

    @abstractmethod
    def query_genre_distribution_by_playlist(self):
        pass

    @abstractmethod
    def query2(self):
        pass

    @abstractmethod
    def query3(self):
        pass

    @abstractmethod
    def query4(self):
        pass

    @abstractmethod
    def query5(self):
        pass

    def queries(self):
        self.query_genre_distribution_by_playlist()

    def run(self):
        self.extract()
        self.transform_and_load()
        self.queries()

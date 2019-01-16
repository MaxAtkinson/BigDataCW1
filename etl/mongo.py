import csv
from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor

class MongoETL(ETL):
    def extract(self):
        cursor.execute('SELECT * FROM Album;')
        with open('data/mongo/Album.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(cursor.column_names)
            for row in cursor:
                writer.writerow(row)

    def transform(self):
        pass

    def load(self):
        pass

    def queries(self):
        pass
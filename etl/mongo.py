import csv
from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor

class MongoETL(ETL):
    def extract(self):
        self.extract_tracks()

    def extract_tracks(self):
        cursor.execute('''
            SELECT t.*,
                m.Name AS MediaTypeName,
                g.Name AS GenreName
            FROM Track t, Genre g, MediaType m
            WHERE t.GenreId = g.GenreId
            AND t.MediaTypeId = m.MediaTypeId;''')
        self.write_query_to_file(cursor, 'tracks')

    def transform(self):
        pass

    def load(self):
        pass

    def queries(self):
        pass

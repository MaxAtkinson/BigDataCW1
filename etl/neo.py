import os
from etl.etl import ETL
from db.neo import db

class NeoETL(ETL):
    def extract(self):
        pass

    def transform_and_load(self):
        if not os.path.isfile('/var/lib/neo4j/import/tracks.csv'):
            p = os.popen('ln ./data/mongo/* /var/lib/neo4j/import')
        with db() as session:
            session.run('MATCH (t:Track) DELETE t')
        self.transform_and_load_tracks()
        # self.transform_and_load_playlists()

    def transform_and_load_tracks(self):
        with db() as session:
            query = '''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///tracks.csv'
                AS row
                CREATE (:Track {
                    id: toInt(row.TrackId),
                    name: row.Name,
                    albumId: toInt(row.AlbumId),
                    mediaTypeId: toInt(row.MediaTypeId),
                    mediaTypeName: row.MediaTypeName,
                    genreId: toInt(row.GenreId),
                    genreName: row.GenreName,
                    composer: row.Composer,
                    ms: toInt(row.Milliseconds),
                    bytes: toInt(row.Bytes),
                    unitPrice: toFloat(row.UnitPrice)
                }); 
            '''
            session.run(query)
            session.run('CREATE INDEX ON :Track(id)')

    def query_genre_distribution_by_playlist(self):
        pass

    def query2(self):
        pass

    def query3(self):
        pass

    def query4(self):
        pass

    def query5(self):
        pass

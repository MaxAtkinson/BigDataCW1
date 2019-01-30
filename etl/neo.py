import os
import constants
from etl.etl import ETL
from db.neo import db

class NeoETL(ETL):

    def extract(self):
        pass

    def transform_and_load(self):
        # with db() as session:

        #     session.run('MATCH (t:Track) DELETE t')
        #     session.run('MATCH (p:Playlist) DELETE p')
        if not os.path.isfile('/var/lib/neo4j/import/tracks.csv'):
            p = os.popen('ln ./data/mongo/* /var/lib/neo4j/import')
        self.transform_and_load_tracks()
        self.transform_and_load_playlists()
        self.transform_and_load_artists()
        self.transform_and_load_albums()

    def transform_and_load_albums(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.ALBUMS_FILENAME}.csv'
                AS row
                CREATE (:Album {{
                    id: toInt(row.AlbumId),
                    title: row.Title,
                    artistId: toInt(row.ArtistId)
                }});
            ''')
            session.run('CREATE INDEX ON :Album(id)')
            session.run('''
                MATCH (ar:Artist)
                MATCH (al:Album) WHERE al.artistId = ar.id
                MERGE (al)-[:BY]->(ar)
            ''')

    def transform_and_load_artists(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.ARTISTS_FILENAME}.csv'
                AS row
                CREATE (:Artist {{
                    id: toInt(row.ArtistId),
                    name: row.Name
                }})
            ''')
            session.run('CREATE INDEX ON :Artist(id)')

    def transform_and_load_tracks(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.TRACKS_FILENAME}.csv'
                AS row
                CREATE (:Track {{
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
                }}); 
            ''')
            session.run('CREATE INDEX ON :Track(id)')

    def transform_and_load_playlists(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.PLAYLISTS_FILENAME}.csv'
                AS row
                CREATE (:Playlist {{
                    id: toInt(row.PlaylistId),
                    name: row.Name
                }}); 
            ''')
            session.run('CREATE INDEX ON :Playlist(id)')
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.PLAYLIST_TRACKS_FILENAME}.csv'
                AS row
                MATCH (t:Track {{id: toInt(row.TrackId)}}), 
                    (p:Playlist {{id: toInt(row.PlaylistId)}})
                MERGE (p)-[:CONTAINS]->(t);
            ''')

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

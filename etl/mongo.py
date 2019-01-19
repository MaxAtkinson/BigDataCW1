import csv
from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor

TRACKS_FILENAME = 'tracks'
PLAYLISTS_FILENAME = 'playlists'
INVOICES_FILENAME = 'invoices'
EMPLOYEES_FILENAME = 'employees'
CUSTOMERS_FILENAME = 'customers'
ARTISTS_FILENAME = 'artists'

class MongoETL(ETL):

    def extract_tracks(self):
        cursor.execute('''
            SELECT t.*,
                m.Name AS MediaTypeName,
                g.Name AS GenreName
            FROM Track t, Genre g, MediaType m
            WHERE t.GenreId = g.GenreId
            AND t.MediaTypeId = m.MediaTypeId
            ORDER BY t.TrackId;''')
        self.write_query_to_file(cursor, TRACKS_FILENAME)

    def extract_playlists(self):
        cursor.execute('''
            SELECT p.*,
                x.TrackIds
            FROM Playlist p
            LEFT JOIN (
                SELECT pt.PlaylistId,
                    GROUP_CONCAT(pt.TrackId) AS TrackIds
                FROM PlaylistTrack pt 
                GROUP BY pt.PlaylistId) x
            ON x.PlaylistId = p.PlaylistId;''')
        self.write_query_to_file(cursor, PLAYLISTS_FILENAME)

    def extract_invoices(self):
        cursor.execute('''
            SELECT i.*,
                il.TrackId,
                il.InvoiceLineId,
                il.UnitPrice,
                il.Quantity
            FROM Invoice i, InvoiceLine il
            WHERE i.InvoiceId = il.InvoiceId
            ORDER BY i.InvoiceId;''')
        self.write_query_to_file(cursor, INVOICES_FILENAME)

    def extract_employees(self):
        cursor.execute('''
            SELECT *
            FROM Employee e
            ORDER BY e.EmployeeId;''')
        self.write_query_to_file(cursor, EMPLOYEES_FILENAME)


    def extract_customers(self):
        cursor.execute('''
            SELECT *
            FROM Customer c
            ORDER BY c.CustomerId;''')
        self.write_query_to_file(cursor, CUSTOMERS_FILENAME)

    def extract_artists(self):
        cursor.execute('''
            SELECT ar.*,
                al.Title AS AlbumTitle,
                al.AlbumId
            FROM Artist ar, Album al
            WHERE al.ArtistId = ar.ArtistId
            ORDER BY ar.ArtistId;''')
        self.write_query_to_file(cursor, ARTISTS_FILENAME)

    def extract(self):
        self.extract_tracks()
        self.extract_playlists()
        self.extract_invoices()
        self.extract_customers()
        self.extract_employees()
        self.extract_artists()

    def transform_and_load(self):
        self.transform_and_load_tracks()
        self.transform_and_load_playlists()
    
    def transform_and_load_tracks(self):
        with open(f'data/mongo/{TRACKS_FILENAME}.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                track = dict(zip(headers, row))
                track = {
                    '_id': int(row[headers.index('TrackId')]),
                    'name': row[headers.index('Name')],
                    'albumId': int(row[headers.index('AlbumId')]),
                    'mediaType': {
                        '_id': int(row[headers.index('MediaTypeId')]),
                        'name': row[headers.index('MediaTypeName')]
                    },
                    'genre': {
                        '_id': int(row[headers.index('GenreId')]),
                        'name': row[headers.index('GenreName')]
                    },
                    'composer': row[headers.index('Composer')],
                    'ms': int(row[headers.index('Milliseconds')]),
                    'bytes': int(row[headers.index('Bytes')]),
                    'unitPrice': float(row[headers.index('UnitPrice')])}
                self.load('tracks', track)

    def transform_and_load_playlists(self):
        with open(f'data/mongo/{PLAYLISTS_FILENAME}.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                track_ids = row[headers.index('TrackIds')].split(',')
                playlist = {
                    '_id': int(row[headers.index('PlaylistId')]),
                    'name': row[headers.index('Name')],
                    'trackIds': [int(track_id) for track_id in track_ids if track_id != '']}
                self.load('playlists', playlist)

    def load(self, collection, document):
        db[collection].insert_one(document)

    def queries(self):
        pass

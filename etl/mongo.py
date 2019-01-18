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
            SELECT p.*, pt.TrackId
            FROM Playlist p
            LEFT JOIN PlaylistTrack pt 
            ON (p.PlaylistId = pt.PlaylistId)
            ORDER BY p.PlaylistId;
            ''')
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
        # self.extract_tracks()
        # self.transform_and_load_tracks()

        self.extract_playlists()
        self.transform_and_load_playlists()
        self.extract_invoices()
        self.extract_customers()
        self.extract_employees()
        self.extract_artists()

    def transform(self):
        pass
    
    def transform_and_load_tracks(self):
        with open(f'data/mongo/{TRACKS_FILENAME}.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            print(headers)
            for row in reader:
                track = dict(zip(headers, row))
                track = {
                    '_id': int(row[headers.index('TrackId')]),
                    'name': row[headers.index('Name')],
                    'albumId': int(row[headers.index('AlbumId')]),
                    'mediaType': {
                        '_id': int(row[3]),
                        'name': row[9]
                    },
                    'genre': {
                        '_id': int(row[4]),
                        'name': row[10]
                    },
                    'composer': row[5],
                    'ms': int(row[6]),
                    'bytes': int(row[7]),
                    'unitPrice': float(row[8])}
                self.load_track(track)

    def load_track(self, track):
        db.tracks.insert_one(track)

    def transform_and_load_playlists(self):
        with open(f'data/mongo/{PLAYLISTS_FILENAME}.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            print(headers)
            current_id = None
            previous_id = None
            playlist = {
                '_id': None,
                'tracks': [],
                'name': None
            }
            for row in reader:
                current_id = row[headers.index('PlaylistId')]
                playlist['name'] = row[headers.index('Name')]
                playlist['_id'] = current_id
                playlist['tracks'].append(row[headers.index('TrackId')])
                if current_id != previous_id:
                    db.playlists.insert_one(playlist)
                previous_id = current_id
            # for row in reader:
            #     track = dict(zip(headers, row))
            #     track = {
            #         '_id': int(row[headers.index('TrackId')]),
            #         'name': row[headers.index('Name')],
            #         'albumId': int(row[headers.index('AlbumId')]),
            #         'mediaType': {
            #             '_id': int(row[3]),
            #             'name': row[9]
            #         },
            #         'genre': {
            #             '_id': int(row[4]),
            #             'name': row[10]
            #         },
            #         'composer': row[5],
            #         'ms': int(row[6]),
            #         'bytes': int(row[7]),
            #         'unitPrice': float(row[8])}
            #     self.load_track(track)

    def load(self):
        pass

    def queries(self):
        pass

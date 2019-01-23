import csv
from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor
from dateutil.parser import parse

TRACKS_FILENAME = 'tracks'
PLAYLISTS_FILENAME = 'playlists'
PLAYLIST_TRACKS_FILENAME = 'playlistTracks'
INVOICES_FILENAME = 'invoices'
INVOICE_LINES_FILENAME = 'invoiceLines'
EMPLOYEES_FILENAME = 'employees'
CUSTOMERS_FILENAME = 'customers'
ARTISTS_FILENAME = 'artists'
ALBUMS_FILENAME = 'albums'

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
                COUNT(pt.TrackId) AS NumTracks
            FROM Playlist p
            LEFT JOIN PlaylistTrack pt
            ON (p.PlaylistId = pt.PlaylistId)
            GROUP BY p.PlaylistId
            ORDER BY p.PlaylistId;''')
        self.write_query_to_file(cursor, PLAYLISTS_FILENAME)

    def extract_playlist_tracks(self):
        cursor.execute('''
            SELECT *
            FROM PlaylistTrack
            ORDER BY PlaylistId;''')
        self.write_query_to_file(cursor, PLAYLIST_TRACKS_FILENAME)

    def extract_invoices(self):
        cursor.execute('''
            SELECT i.*,
                COUNT(il.InvoiceId) AS NumLines
            FROM Invoice i, InvoiceLine il
            WHERE i.InvoiceId = il.InvoiceId
            GROUP BY i.InvoiceId
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

    def extract_albums(self):
        cursor.execute('''
            SELECT *
            FROM Album
            ORDER BY ArtistId;''')
        self.write_query_to_file(cursor, ALBUMS_FILENAME)

    def extract_artists(self):
        cursor.execute('''
            SELECT ar.*,
                COUNT(al.ArtistId) AS NumAlbums
            FROM Artist ar
            LEFT JOIN Album al
            ON (ar.ArtistId = al.ArtistId)
            GROUP BY ar.ArtistId
            ORDER BY ar.ArtistId;''')
        self.write_query_to_file(cursor, ARTISTS_FILENAME)

    def extract_invoice_lines(self):
        cursor.execute('''
            SELECT *
            FROM InvoiceLine
            ORDER BY InvoiceId;''')
        self.write_query_to_file(cursor, INVOICE_LINES_FILENAME)

    def extract(self):
        self.extract_tracks()
        self.extract_playlists()
        self.extract_playlist_tracks()
        self.extract_artists()
        self.extract_albums()
        self.extract_invoices()
        self.extract_invoice_lines()
        self.extract_customers()
        self.extract_employees()

    def transform_and_load(self):
        self.transform_and_load_tracks()
        self.transform_and_load_playlists()
        self.transform_and_load_artists()
        self.transform_and_load_invoices()
        self.transform_and_load_customers()
        self.transform_and_load_employees()

    def transform_and_load_employees(self):
        with open(f'data/mongo/{EMPLOYEES_FILENAME}.csv', 'r') as f:
            def first_lower(s): return s[0].lower() + s[1:]
            reader = csv.reader(f)
            headers = [first_lower(header) for header in next(reader)]
            for row in reader:
                employee = dict(zip(headers, row))
                employee['_id'] = int(employee['employeeId'])
                employee.pop('employeeId', None)
                employee['reportsTo'] = int(employee['reportsTo']) if employee['reportsTo'].isdigit() else None  
                employee['hireDate'] = parse(employee['hireDate'])
                employee['birthDate'] = parse(employee['birthDate'])
                self.load(EMPLOYEES_FILENAME, employee)

    def transform_and_load_customers(self):
        with open(f'data/mongo/{CUSTOMERS_FILENAME}.csv', 'r') as f:
            reader = csv.reader(f)
            customer_headers = next(reader)
            for row in reader:
                customer = {
                    '_id': int(row[customer_headers.index('CustomerId')]),
                    'firstName': row[customer_headers.index('FirstName')],
                    'lastName': row[customer_headers.index('LastName')],
                    'company': row[customer_headers.index('Company')],
                    'address': row[customer_headers.index('Address')],
                    'city': row[customer_headers.index('City')],
                    'state': row[customer_headers.index('State')],
                    'country': row[customer_headers.index('Country')],
                    'postalCode': row[customer_headers.index('PostalCode')],
                    'phone': row[customer_headers.index('Phone')],
                    'fax': row[customer_headers.index('Fax')],
                    'email': row[customer_headers.index('Email')],
                    'supportRepId': int(row[customer_headers.index('SupportRepId')])}
                self.load(CUSTOMERS_FILENAME, customer)

    def transform_and_load_invoices(self):
        invoice_path = f'data/mongo/{INVOICES_FILENAME}.csv'
        invoice_lines_path = f'data/mongo/{INVOICE_LINES_FILENAME}.csv'
        with open(invoice_path, 'r') as i, open(invoice_lines_path) as il:
            invoice_reader, invoice_lines_reader = csv.reader(i), csv.reader(il)
            invoice_headers, invoice_lines_headers = next(invoice_reader), next(invoice_lines_reader)
            for row in invoice_reader:
                invoice_date = parse(row[invoice_headers.index('InvoiceDate')])
                invoice = {
                    '_id': int(row[invoice_headers.index('InvoiceId')]),
                    'customerId': int(row[invoice_headers.index('CustomerId')]),
                    'invoiceDate': invoice_date,
                    'billingAddress': row[invoice_headers.index('BillingAddress')],
                    'billingCity': row[invoice_headers.index('BillingCity')],
                    'billingState': row[invoice_headers.index('BillingState')],
                    'billingCountry': row[invoice_headers.index('BillingCountry')],
                    'billingPostalCode': row[invoice_headers.index('BillingPostalCode')],
                    'total': float(row[invoice_headers.index('Total')])}
                invoice_lines = []
                for _ in range(int(row[invoice_headers.index('NumLines')])):
                    invoice_lines_row = next(invoice_lines_reader)
                    invoice_lines.append({
                            '_id': int(invoice_lines_row[invoice_lines_headers.index('InvoiceLineId')]),
                            'invoiceId': int(invoice_lines_row[invoice_lines_headers.index('InvoiceId')]),
                            'trackId': int(invoice_lines_row[invoice_lines_headers.index('TrackId')]),
                            'unitPrice': float(invoice_lines_row[invoice_lines_headers.index('UnitPrice')]),
                            'quantity': int(invoice_lines_row[invoice_lines_headers.index('Quantity')])
                        })
                invoice['invoiceLines'] = invoice_lines
                self.load(INVOICES_FILENAME, invoice)
    
    def transform_and_load_tracks(self):
        with open(f'data/mongo/{TRACKS_FILENAME}.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
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
                self.load(TRACKS_FILENAME, track)

    def transform_and_load_playlists(self):
        playlist_path = f'data/mongo/{PLAYLISTS_FILENAME}.csv'
        playlist_track_path = f'data/mongo/{PLAYLIST_TRACKS_FILENAME}.csv'
        with open(playlist_path, 'r') as p, open(playlist_track_path) as pt:
            playlist_reader, playlist_tracks_reader = csv.reader(p), csv.reader(pt)
            playlist_headers, playlist_tracks_headers = next(playlist_reader), next(playlist_tracks_reader)
            for row in playlist_reader:
                playlist = {
                    '_id': int(row[playlist_headers.index('PlaylistId')]),
                    'name': row[playlist_headers.index('Name')],
                    'trackIds': []}
                for _ in range(int(row[playlist_headers.index('NumTracks')])):
                    playlist_track = next(playlist_tracks_reader)
                    playlist['trackIds'].append(int(playlist_track[playlist_tracks_headers.index('TrackId')]))
                self.load(PLAYLISTS_FILENAME, playlist)

    def transform_and_load_artists(self):
        artists_path = f'data/mongo/{ARTISTS_FILENAME}.csv'
        albums_path = f'data/mongo/{ALBUMS_FILENAME}.csv'
        with open(artists_path, 'r') as ar, open(albums_path) as al:
            artist_reader, album_reader = csv.reader(ar), csv.reader(al)
            artist_headers, album_headers = next(artist_reader), next(album_reader)
            for row in artist_reader:
                artist = {
                    '_id': int(row[artist_headers.index('ArtistId')]),
                    'name': row[artist_headers.index('Name')],
                    'albums': []}
                for _ in range(int(row[artist_headers.index('NumAlbums')])):
                    album = next(album_reader)
                    artist['albums'].append({
                        '_id': int(album[album_headers.index('AlbumId')]),
                        'title': album[album_headers.index('Title')],
                        'artistId': int(album[album_headers.index('ArtistId')])})
                self.load(ARTISTS_FILENAME, artist)

    def load(self, collection, document):
        db[collection].insert_one(document)

    def queries(self):
        pass

import csv
import pprint
from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor
from dateutil.parser import parse
import constants

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
        self.write_query_to_file(cursor, constants.TRACKS_FILENAME)

    def extract_playlists(self):
        cursor.execute('''
            SELECT p.*,
                COUNT(pt.TrackId) AS NumTracks
            FROM Playlist p
            LEFT JOIN PlaylistTrack pt
            ON (p.PlaylistId = pt.PlaylistId)
            GROUP BY p.PlaylistId
            ORDER BY p.PlaylistId;''')
        self.write_query_to_file(cursor, constants.PLAYLISTS_FILENAME)

    def extract_playlist_tracks(self):
        cursor.execute('''
            SELECT *
            FROM PlaylistTrack
            ORDER BY PlaylistId;''')
        self.write_query_to_file(cursor, constants.PLAYLIST_TRACKS_FILENAME)

    def extract_invoices(self):
        cursor.execute('''
            SELECT i.*,
                COUNT(il.InvoiceId) AS NumLines
            FROM Invoice i, InvoiceLine il
            WHERE i.InvoiceId = il.InvoiceId
            GROUP BY i.InvoiceId
            ORDER BY i.InvoiceId;''')
        self.write_query_to_file(cursor, constants.INVOICES_FILENAME)

    def extract_employees(self):
        cursor.execute('''
            SELECT *
            FROM Employee e
            ORDER BY e.EmployeeId;''')
        self.write_query_to_file(cursor, constants.EMPLOYEES_FILENAME)


    def extract_customers(self):
        cursor.execute('''
            SELECT *
            FROM Customer c
            ORDER BY c.CustomerId;''')
        self.write_query_to_file(cursor, constants.CUSTOMERS_FILENAME)

    def extract_albums(self):
        cursor.execute('''
            SELECT *
            FROM Album
            ORDER BY ArtistId;''')
        self.write_query_to_file(cursor, constants.ALBUMS_FILENAME)

    def extract_artists(self):
        cursor.execute('''
            SELECT ar.*,
                COUNT(al.ArtistId) AS NumAlbums
            FROM Artist ar
            LEFT JOIN Album al
            ON (ar.ArtistId = al.ArtistId)
            GROUP BY ar.ArtistId
            ORDER BY ar.ArtistId;''')
        self.write_query_to_file(cursor, constants.ARTISTS_FILENAME)

    def extract_invoice_lines(self):
        cursor.execute('''
            SELECT *
            FROM InvoiceLine
            ORDER BY InvoiceId;''')
        self.write_query_to_file(cursor, constants.INVOICE_LINES_FILENAME)

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
        with open(f'data/mongo/{constants.EMPLOYEES_FILENAME}.csv', 'r') as f:
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
                self.load(constants.EMPLOYEES_FILENAME, employee)

    def transform_and_load_customers(self):
        with open(f'data/mongo/{constants.CUSTOMERS_FILENAME}.csv', 'r') as f:
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
                self.load(constants.CUSTOMERS_FILENAME, customer)

    def transform_and_load_invoices(self):
        invoice_path = f'data/mongo/{constants.INVOICES_FILENAME}.csv'
        invoice_lines_path = f'data/mongo/{constants.INVOICE_LINES_FILENAME}.csv'
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
                self.load(constants.INVOICES_FILENAME, invoice)
    
    def transform_and_load_tracks(self):
        with open(f'data/mongo/{constants.TRACKS_FILENAME}.csv', 'r') as f:
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
                self.load(constants.TRACKS_FILENAME, track)

    def transform_and_load_playlists(self):
        playlist_path = f'data/mongo/{constants.PLAYLISTS_FILENAME}.csv'
        playlist_track_path = f'data/mongo/{constants.PLAYLIST_TRACKS_FILENAME}.csv'
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
                self.load(constants.PLAYLISTS_FILENAME, playlist)

    def transform_and_load_artists(self):
        artists_path = f'data/mongo/{constants.ARTISTS_FILENAME}.csv'
        albums_path = f'data/mongo/{constants.ALBUMS_FILENAME}.csv'
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
                self.load(constants.ARTISTS_FILENAME, artist)

    def load(self, collection, document):
        db[collection].insert_one(document)

    def query_genre_distribution_by_playlist(self):
        try:
            genre_distribution_for_playlist = db.playlists.aggregate([
                {
                    '$match': {
                        '_id': 1
                    }
                },
                {
                    '$lookup': {
                        'from': 'tracks',
                        'as': 'tracks',
                        'localField': 'trackIds',
                        'foreignField': '_id'
                    }
                },
                {
                    '$project': {'trackIds': False}
                },
                {
                    '$unwind': '$tracks'
                },
                {
                    '$sortByCount': '$tracks.genre.name'
                }
            ])
            for x in genre_distribution_for_playlist:
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(x)
        except Exception:
            self.query_genre_distribution_by_playlist()

    def query2(self):
        try:
            best_employee = db.employees.aggregate([
                {
                    '$lookup': { 
                        'from': 'customers',
                        'as': 'customers',
                        'localField': '_id',
                        'foreignField': 'supportRepId' 
                    }
                },
                {
                    '$unwind': '$customers'
                },
                {
                    '$lookup': {
                        'from': 'invoices',
                        'as': 'invoices',
                        'localField': 'customers._id',
                        'foreignField': 'customerId'
                    }
                },
                {
                    '$unwind': '$invoices'
                },
                {
                    '$group': {
                        '_id': '$_id',
                        'total': {
                            '$sum' : '$invoices.total' 
                        }
                    }
                },
                {
                    '$sort': {
                        'total': -1
                    }
                },
                {
                    '$limit': 1
                }
            ])
            for x in best_employee:
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(x)
        except Exception:
            self.query2()
    
    def query3(self):
        try:
            best_track = db.invoices.aggregate([
                {
                    '$unwind': '$invoiceLines'
                },
                {
                    '$lookup': {
                        'from': 'tracks',
                        'as': 'tracks',
                        'localField': 'invoiceLines.trackId',
                        'foreignField': '_id'
                    }  
                },
                {
                    '$unwind': '$tracks'
                },
                {
                    '$group': {
                        '_id': '$tracks._id',
                        'total': {
                            '$sum' : {
                                '$multiply': ['$invoiceLines.quantity', '$invoiceLines.unitPrice']
                            } 
                        }
                    }
                },
                {
                    '$sort': {
                        'total': -1
                    }
                },
                {
                    '$limit': 10
                }
            ])
            for x in best_track:
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(x)
        except Exception:
            self.query3()        

    def query4(self):
        try:
            most_playlisted_artist = db.playlists.aggregate([
                {
                    '$unwind': '$trackIds'
                },
                {
                    '$lookup': {
                        'from': 'tracks',
                        'as': 'tracks',
                        'localField': 'trackIds',
                        'foreignField': '_id'
                    }  
                },
                {
                    '$lookup': {
                        'from': 'artists',
                        'as': 'artists',
                        'localField': 'tracks.albumId',
                        'foreignField': 'albums._id'
                    }  
                },
                {
                    '$unwind': '$artists'
                },
                {
                    '$group': {
                        '_id': '$artists._id',
                        'count': {
                            '$sum' : 1
                        }
                    }
                },
                {
                    '$sort': {
                        'count': -1
                    }
                },
                {
                    '$limit': 10
                }
            ])
            for x in most_playlisted_artist:
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(x)
        except Exception:
            self.query4() 

    def query5(self):
        favourite_artist_by_region = db.invoices.aggregate([
            {
                '$unwind': '$invoiceLines'
            },
            {
                '$lookup': {
                    'from': 'tracks',
                    'as': 'tracks',
                    'localField': 'invoiceLines.trackId',
                    'foreignField': '_id'
                }  
            },
            {
                '$unwind': '$tracks'
            },
            {
                '$lookup': {
                    'from': 'artists',
                    'as': 'artists',
                    'localField': 'tracks.albumId',
                    'foreignField': 'albums._id'
                }  
            },
            {
                '$unwind': '$artists'
            },
            {
                '$group': {
                    '_id': {
                        'country': '$billingCountry',
                        'artists': '$artists._id',
                        'name': '$artists.name'

                    },
                    'total': {
                        '$sum': 1
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.country',
                    'details': {
                        '$push': {
                            'total':'$total',
                            'artist': '$_id.artists',
                            'name': '$_id.name'
                        }
                    }
                } 
            },
            {'$project': 
                {
                    '_id': 1,
                    'maxPerCountry': {
                        "$max": "$details.total"
                    },
                    'details': 1
                }
            },
            { '$project': 
                {
                'topArtists': {
                    '$filter': {
                        'input': "$details",
                        'as': "details",
                        'cond': { '$gte': [ "$$details.total", '$maxPerCountry' ] }
                        }
                    }
                }
            }
        ])
        for x in favourite_artist_by_region:
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(x)            

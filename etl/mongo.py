import csv
import pprint
from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor
from dateutil.parser import parse
import constants

class MongoETL(ETL):
    ''' ETL class for transforming and loading data into our MongoDB schema. '''

    def transform_and_load(self):
        ''' Starts the transformation and loading process into our MongoDB schema. '''
        self.transform_and_load_tracks()
        self.transform_and_load_playlists()
        self.transform_and_load_artists()
        self.transform_and_load_invoices()
        self.transform_and_load_customers()
        self.transform_and_load_employees()

    def transform_and_load_employees(self):
        with open(f'data/{constants.EMPLOYEES_FILENAME}.csv', 'r') as f:
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
        with open(f'data/{constants.CUSTOMERS_FILENAME}.csv', 'r') as f:
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
        invoice_path = f'data/{constants.INVOICES_FILENAME}.csv'
        invoice_lines_path = f'data/{constants.INVOICE_LINES_FILENAME}.csv'
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
        with open(f'data/{constants.TRACKS_FILENAME}.csv', 'r') as f:
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
        playlist_path = f'data/{constants.PLAYLISTS_FILENAME}.csv'
        playlist_track_path = f'data/{constants.PLAYLIST_TRACKS_FILENAME}.csv'
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
        artists_path = f'data/{constants.ARTISTS_FILENAME}.csv'
        albums_path = f'data/{constants.ALBUMS_FILENAME}.csv'
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
        ''' Utility method for loading into MongoDB collections simply. '''
        db[collection].insert_one(document)

    def query_genre_distribution_by_playlist(self, playlist_id=1):
        ''' Given a playlist id, this query will give the distribution of different genres in the playlist, sorted descending. '''
        genre_distribution_for_playlist = db.playlists.aggregate([
            # Match playlist on some id.
            {
                '$match': {
                    '_id': playlist_id
                }
            },
            # 'Join' the tracks table using playlists.trackIds.
            {
                '$lookup': {
                    'from': 'tracks',
                    'as': 'tracks',
                    'localField': 'trackIds',
                    'foreignField': '_id'
                }
            },
            # Remove the trackIds field - we have no use for it now, the join is done.
            {
                '$project': {'trackIds': False}
            },
            # Unwind (flatten) the tracks array from our $lookup.
            {
                '$unwind': '$tracks'
            },
            # Groups by genre name and sorts by the count descending.
            {
                '$sortByCount': '$tracks.genre.name'
            }
        ])
        for row in genre_distribution_for_playlist:
            print(f'This playlist contains {row["count"]} {row["_id"]} songs.')

    def best_employee(self):
        ''' This query will find the employee who has brought in the most revenue. '''
        best_employee = db.employees.aggregate([
            # 'Join' from employee to customers on supportRepId.
            {
                '$lookup': { 
                    'from': 'customers',
                    'as': 'customers',
                    'localField': '_id',
                    'foreignField': 'supportRepId' 
                }
            },
            # Unwind (flatten) the customers array from $lookup.
            {
                '$unwind': '$customers'
            },
            # 'Join' onto invoices from customers.
            {
                '$lookup': {
                    'from': 'invoices',
                    'as': 'invoices',
                    'localField': 'customers._id',
                    'foreignField': 'customerId'
                }
            },
            # Unwind (flatten) the invoices array from $lookup.
            {
                '$unwind': '$invoices'
            },
            # Group by employee, sum their total invoices.
            {
                '$group': {
                    '_id': '$_id',
                    'total': {
                        '$sum' : '$invoices.total' 
                    }
                }
            },
            # Sort by the aggregate sum descending.
            {
                '$sort': {
                    'total': -1
                }
            },
            # Return only the best employee.
            {
                '$limit': 1
            }
        ])
        for row in best_employee:
            print(f'The best employee is #{row["_id"]}.')
    
    def highest_grossing_tracks(self):
        ''' This query will find the 10 highest grossing tracks. '''
        best_tracks = db.invoices.aggregate([
            # Unwind (flatten) invoiceLines array.
            {
                '$unwind': '$invoiceLines'
            },
            # 'Join' onto the tracks table using trackId.
            {
                '$lookup': {
                    'from': 'tracks',
                    'as': 'tracks',
                    'localField': 'invoiceLines.trackId',
                    'foreignField': '_id'
                }  
            },
            # Unwind (flatten) tracks array from $lookup.
            {
                '$unwind': '$tracks'
            },
            # Group by the track id and calculate the total generated by all invoice lines of that track.
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
            # Sort by the aggregate sum descending.
            {
                '$sort': {
                    'total': -1
                }
            },
            # Take the top 10 grossing tracks.
            {
                '$limit': 10
            }
        ])
        for row in best_tracks:
            print(f'Track #{row["_id"]} generated £{row["total"]}.')

    def most_playlisted_artists(self):
        ''' This query will find the top 10 most playlisted artists. '''
        most_playlisted_artists = db.playlists.aggregate([
            # Unwind (flatten) the trackIds array.
            {
                '$unwind': '$trackIds'
            },
            # 'Join' the tracks playlist on trackIds.
            {
                '$lookup': {
                    'from': 'tracks',
                    'as': 'tracks',
                    'localField': 'trackIds',
                    'foreignField': '_id'
                }  
            },
            # 'Join' the artists collection using tracks.albumId.
            {
                '$lookup': {
                    'from': 'artists',
                    'as': 'artists',
                    'localField': 'tracks.albumId',
                    'foreignField': 'albums._id'
                }  
            },
            # Unwind (flatten) the artists array from $lookup.
            {
                '$unwind': '$artists'
            },
            # Group by artist id, count the artists appearance.
            {
                '$group': {
                    '_id': '$artists._id',
                    'count': {
                        '$sum' : 1
                    }
                }
            },
            # Sort by the aggregate count descending.
            {
                '$sort': {
                    'count': -1
                }
            },
            # Return the top 10.
            {
                '$limit': 10
            }
        ])
        for row in most_playlisted_artists:
            print(f'Artist #{row["_id"]} has been playlisted {row["count"]} times.')

    def favourite_artist_by_region(self):
        ''' This query will display the favourite artist for each invoice region. '''
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
        for row in favourite_artist_by_region:
            artists = [artist['name'] for artist in row['topArtists']]
            print(f'The most popular Artists in {row["_id"]} are {", ".join(artists)}.')
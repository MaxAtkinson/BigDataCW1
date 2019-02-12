import csv
from abc import ABC, abstractmethod
from db.mysql import cursor
import constants

class ETL(ABC):
    ''' Abstract base class for ETL jobs to improve scalability. To perform a new ETL job, subclass this. '''

    @staticmethod
    def write_query_to_file(cursor, entity_name):
        with open(f'data/{entity_name}.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(cursor.column_names)
            for row in cursor:
                writer.writerow(row)

    @staticmethod
    def extract_tracks():
        cursor.execute('''
            SELECT t.*,
                m.Name AS MediaTypeName,
                g.Name AS GenreName
            FROM Track t, Genre g, MediaType m
            WHERE t.GenreId = g.GenreId
            AND t.MediaTypeId = m.MediaTypeId
            ORDER BY t.TrackId;''')
        ETL.write_query_to_file(cursor, constants.TRACKS_FILENAME)

    @staticmethod
    def extract_playlists():
        cursor.execute('''
            SELECT p.*,
                COUNT(pt.TrackId) AS NumTracks
            FROM Playlist p
            LEFT JOIN PlaylistTrack pt
            ON (p.PlaylistId = pt.PlaylistId)
            GROUP BY p.PlaylistId
            ORDER BY p.PlaylistId;''')
        ETL.write_query_to_file(cursor, constants.PLAYLISTS_FILENAME)

    @staticmethod
    def extract_playlist_tracks():
        cursor.execute('''
            SELECT *
            FROM PlaylistTrack
            ORDER BY PlaylistId;''')
        ETL.write_query_to_file(cursor, constants.PLAYLIST_TRACKS_FILENAME)

    @staticmethod
    def extract_invoices():
        cursor.execute('''
            SELECT i.*,
                COUNT(il.InvoiceId) AS NumLines
            FROM Invoice i, InvoiceLine il
            WHERE i.InvoiceId = il.InvoiceId
            GROUP BY i.InvoiceId
            ORDER BY i.InvoiceId;''')
        ETL.write_query_to_file(cursor, constants.INVOICES_FILENAME)

    @staticmethod
    def extract_employees():
        cursor.execute('''
            SELECT *
            FROM Employee e
            ORDER BY e.EmployeeId;''')
        ETL.write_query_to_file(cursor, constants.EMPLOYEES_FILENAME)


    @staticmethod
    def extract_customers():
        cursor.execute('''
            SELECT *
            FROM Customer c
            ORDER BY c.CustomerId;''')
        ETL.write_query_to_file(cursor, constants.CUSTOMERS_FILENAME)

    @staticmethod
    def extract_albums():
        cursor.execute('''
            SELECT *
            FROM Album
            ORDER BY ArtistId;''')
        ETL.write_query_to_file(cursor, constants.ALBUMS_FILENAME)

    @staticmethod
    def extract_artists():
        cursor.execute('''
            SELECT ar.*,
                COUNT(al.ArtistId) AS NumAlbums
            FROM Artist ar
            LEFT JOIN Album al
            ON (ar.ArtistId = al.ArtistId)
            GROUP BY ar.ArtistId
            ORDER BY ar.ArtistId;''')
        ETL.write_query_to_file(cursor, constants.ARTISTS_FILENAME)

    @staticmethod
    def extract_invoice_lines():
        cursor.execute('''
            SELECT *
            FROM InvoiceLine
            ORDER BY InvoiceId;''')
        ETL.write_query_to_file(cursor, constants.INVOICE_LINES_FILENAME)

    @staticmethod
    def extract():
        ''' Performs extraction of the MySQL database into .csv files ready for transformation. '''
        ETL.extract_tracks()
        ETL.extract_playlists()
        ETL.extract_playlist_tracks()
        ETL.extract_artists()
        ETL.extract_albums()
        ETL.extract_invoices()
        ETL.extract_invoice_lines()
        ETL.extract_customers()
        ETL.extract_employees()

    @abstractmethod
    def transform_and_load(self):
        ''' Abstract method which should be implemented in order to transform and load the extracted data into the new schema. '''
        pass

    @abstractmethod
    def query_genre_distribution_by_playlist(self):
        ''' Abstract method to query the genre distribution for a given playlist. '''
        pass

    @abstractmethod
    def best_employee(self):
        ''' Abstract method to query ___________________. '''
        pass

    @abstractmethod
    def highest_grossing_tracks(self):
        ''' Abstract method to query ___________________. '''
        pass

    @abstractmethod
    def most_playlisted_artists(self):
        ''' Abstract method to query ___________________. '''
        pass

    @abstractmethod
    def favourite_artist_by_region(self):
        ''' Abstract method to query ___________________. '''
        pass

    def queries(self):
        ''' Calls the 5 implemented queries which are implemented as abstract methods on the subclasses. '''
        self.query_genre_distribution_by_playlist()
        self.best_employee()
        self.highest_grossing_tracks()
        self.most_playlisted_artists()
        self.favourite_artist_by_region()

    def run(self):
        ''' Main method. Transforms, loads and queries the databases. '''
        self.transform_and_load()
        self.queries()

from etl.etl import ETL
from db.mongo import db
from db.mysql import cursor

class MongoETL(ETL):
    def extract_tracks(self):
        cursor.execute('''
            SELECT t.*,
                m.Name AS MediaTypeName,
                g.Name AS GenreName
            FROM Track t, Genre g, MediaType m
            WHERE t.GenreId = g.GenreId
            AND t.MediaTypeId = m.MediaTypeId;''')
        self.write_query_to_file(cursor, 'tracks')

    def extract_playlists(self):
        cursor.execute('''
            SELECT p.*, pt.TrackId
            FROM Playlist p, PlaylistTrack pt
            WHERE p.PlaylistId = pt.PlaylistId;''')
        self.write_query_to_file(cursor, 'playlists')

    def extract_invoices(self):
        cursor.execute('''
            SELECT i.*,
                il.TrackId,
                il.InvoiceLineId,
                il.UnitPrice,
                il.Quantity
            FROM Invoice i, InvoiceLine il
            WHERE i.InvoiceId = il.InvoiceId;''')
        self.write_query_to_file(cursor, 'invoices')

    def extract_employees(self):
        cursor.execute('''
            SELECT *
            FROM Employee;''')
        self.write_query_to_file(cursor, 'employees')


    def extract_customers(self):
        cursor.execute('''
            SELECT *
            FROM Customer;''')
        self.write_query_to_file(cursor, 'customers')

    def extract_artists(self):
        cursor.execute('''
            SELECT ar.*,
                al.Title AS AlbumTitle,
                al.AlbumId
            FROM Artist ar, Album al
            WHERE al.ArtistId = ar.ArtistId;''')
        self.write_query_to_file(cursor, 'artists')

    def extract(self):
        self.extract_tracks()
        self.extract_playlists()
        self.extract_invoices()
        self.extract_customers()
        self.extract_employees()
        self.extract_artists()

    def transform(self):
        pass

    def load(self):
        pass

    def queries(self):
        pass

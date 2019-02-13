import os
import constants
from etl.etl import ETL
from db.neo import db

class NeoETL(ETL):
    ''' ETL class for transforming and loading data into our Neo4j schema. '''

    def transform_and_load(self):
        ''' Starts the transformation and loading process into our Neo4j schema. '''
        if not os.path.isfile('/var/lib/neo4j/import/tracks.csv'):
            p = os.popen('ln ./data/* /var/lib/neo4j/import')
        self.transform_and_load_tracks()
        self.transform_and_load_playlists()
        self.transform_and_load_artists()
        self.transform_and_load_albums()
        self.transform_and_load_invoices()
        self.transform_and_load_invoice_lines()
        self.transform_and_load_customers()
        self.transform_and_load_employees()

    def transform_and_load_employees(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.EMPLOYEES_FILENAME}.csv'
                AS row
                CREATE (:Employee {{
                    id: toInt(row.EmployeeId),
                    lastName: row.LastName,
                    firstName: row.FirstName,
                    title: row.Title,
                    reportsTo: toInt(row.ReportsTo),
                    birthDate: substring(row.BirthDate,0,10),
                    hireDate: substring(row.HireDate,0,10),
                    address: row.Address,
                    city: row.City,
                    state: row.State,
                    country: row.Country,
                    postalCode: row.PostalCode,
                    phone: row.Phone,
                    fax: row.Fax,
                    email: row.Email
                }})
            ''')
            session.run('''
                CREATE INDEX ON :Employee(id)
            ''')
            session.run('''
                MATCH (emp:Employee)
                MATCH (cus:Customer) WHERE cus.supportRepId = emp.id
                MERGE (cus)-[:SUPPORTED_BY]->(emp)
            ''')
            session.run('''
                MATCH (emp:Employee)
                MATCH (emp2:Employee) WHERE emp.reportsTo = emp2.id
                MERGE (emp)-[:REPORTS_TO]->(emp2)
            ''')

    def transform_and_load_customers(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.CUSTOMERS_FILENAME}.csv'
                AS row
                CREATE (:Customer {{
                    id: toInt(row.CustomerId),
                    firstName: row.FirstName,
                    lastName: row.LastName,
                    company: row.Company,
                    address: row.Address,
                    city: row.City,
                    state: row.State,
                    country: row.Country,
                    postalCode: row.PostalCode,
                    phone: row.Phone,
                    fax: row.Fax,
                    email: row.Email,
                    supportRepId: toInt(row.SupportRepId)
                }})
            ''')
            session.run('CREATE INDEX ON :Customer(id)')
            session.run('''
                MATCH (cus:Customer)
                MATCH (in:Invoice) WHERE in.customerId = cus.id
                MERGE (in)-[:BILLED_TO]->(cus)
            ''')

    def transform_and_load_invoice_lines(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.INVOICE_LINES_FILENAME}.csv'
                AS row
                CREATE (:InvoiceLine {{
                    id: toInt(row.InvoiceLineId),
                    invoiceId: toInt(row.InvoiceId),
                    trackId: toInt(row.TrackId),
                    unitPrice: toFloat(row.UnitPrice),
                    quantity: toInt(row.Quantity)
                }})
            ''')
            session.run('CREATE INDEX ON :InvoiceLine(id)')
            session.run('''
                MATCH (il:InvoiceLine)
                MATCH (in:Invoice) WHERE il.invoiceId = in.id
                MERGE (il)-[:IN]->(in)
            ''')
            session.run('''
                MATCH (il:InvoiceLine)
                MATCH (track:Track) WHERE il.trackId = track.id
                MERGE (il)-[:FOR]->(track)
            ''')


    def transform_and_load_invoices(self):
        with db() as session:
            session.run(f'''
                USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM 'file:///{constants.INVOICES_FILENAME}.csv'
                AS row
                CREATE (:Invoice {{
                    id: toInt(row.InvoiceId),
                    customerId: toInt(row.CustomerId),
                    date: substring(row.InvoiceDate,0,10),
                    billingAddress: row.BillingAddress,
                    billingState: row.BillingState,
                    billingCountry: row.BillingCountry,
                    billingPostalCode: row.BillingPostalCode,
                    total: toFloat(row.Total)
                }})
            ''')
            session.run('CREATE INDEX ON :Invoice(id)')

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
            session.run('''
                MATCH (track:Track)
                MATCH (al:Album) WHERE track.albumId = al.id
                MERGE (track)-[:ON]->(al)
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
        ''' Given a playlist id, this query will give the distribution of different genres in the playlist, sorted descending. '''
        with db() as session:
            response = session.run('''
            MATCH (p:Playlist {id: 1})-[:CONTAINS]->(t:Track)
            WITH DISTINCT(t.genreName) as genre, COUNT(t) as size
            RETURN genre, size
            ORDER BY size DESC''')
            for node in response:
                print('This playlist contains {size} {genre} songs.'.format(**node))


    def best_employee(self):
        ''' This query will find the employee who has brought in the most revenue. '''
        with db() as session:
            response = session.run('''
            MATCH (emp:Employee)<-[:SUPPORTED_BY]-(cus:Customer)<-[:BILLED_TO]-(i:Invoice)
            WITH DISTINCT(emp) as employee, sum(i.total) as total
            RETURN employee.id as employee, total
            ORDER BY total DESC
            LIMIT 1''')
            for node in response:
                print('The best employee is #{employee}.'.format(**node))

    def highest_grossing_tracks(self):
        ''' This query will find the 10 highest grossing tracks. '''
        with db() as session:
            response = session.run('''
            MATCH (t:Track)<-[:FOR]-(il:InvoiceLine)
            WITH DISTINCT(t) as track, SUM(il.unitPrice * il.quantity) as total
            RETURN total, track.id as track
            ORDER BY total DESC
            LIMIT 10''')
            for node in response:
                print('Track #{track} generated £{total}.'.format(**node))


    def most_playlisted_artists(self):
        ''' This query will find the top 10 most playlisted artists. '''
        with db() as session:
            response = session.run('''
            MATCH (p:Playlist)-[:CONTAINS]->(track:Track)-[:ON]->(album:Album)-[:BY]->(ar:Artist)
            WITH DISTINCT(ar) as artist, COUNT(track) as total
            RETURN artist.id as artist, total
            ORDER BY total DESC
            LIMIT 10''')
            for node in response:
                print('Artist #{artist} has been playlisted {total} times.'.format(**node))


    def favourite_artist_by_region(self):
        ''' This query will display the favourite artist for each invoice region. '''
        with db() as session:
            response = session.run('''
            MATCH (c:Customer)<-[:BILLED_TO]-(i:Invoice)<-[:IN]-(il:InvoiceLine)-[:FOR]->(t:Track)-[:ON]->(al:Album)-[:BY]->(art:Artist)
            WITH distinct(i.billingCountry) as country, COUNT(il) as amount, art.name as name
            WITH {top: MAX(amount), country: country} as topPerCountry
            MATCH (c:Customer)<-[:BILLED_TO]-(i:Invoice)<-[:IN]-(il:InvoiceLine)-[:FOR]->(t:Track)-[:ON]->(al:Album)-[:BY]->(art:Artist)
            WITH distinct(i.billingCountry) as country, COUNT(il) as amount, art.name as name, topPerCountry as topPerCountry
            WHERE amount = topPerCountry.top AND country = topPerCountry.country
            WITH country as country, collect(name) as name, amount
            RETURN country, name, amount
            ''')
            for node in response:
                country = node['country']
                artists = ','.join(node['name'])
                print(f'The most popular Artists in {country} are {artists}.')

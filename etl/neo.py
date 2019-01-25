from etl.etl import ETL
from db.neo import db

class NeoETL(ETL):
    def extract(self):
        query = '''
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 'file:///invoices.csv'
        AS row
        CREATE (:Customer {billingAddress: row.BillingAddress});
        '''            
        db().run(query)

    def transform_and_load(self):
        pass

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

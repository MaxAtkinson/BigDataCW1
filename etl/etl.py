import csv
from abc import ABC, abstractmethod
from db.mysql import cursor

class ETL():
    def __init__(self, jobs):
        self.jobs = jobs
        self.table_names = ['Invoice', 'Customer','Employee',
            'InvoiceLine','Playlist','PlaylistTrack', 'Track',
            'Album', 'Artist', 'MediaType', 'Genre']

    def extract(self):
        for table_name in self.table_names:
            cursor.execute(f'SELECT * FROM {table_name};')
            with open(f'data/{table_name}.csv', 'w') as f:
                writer = csv.writer(f)
                writer.writerow(cursor.column_names)
                for row in cursor:
                    writer.writerow(row)

    def run(self):
        self.extract()
        for job in self.jobs:
            job.run()
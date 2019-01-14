from etl.mongo import MongoETL
from etl.neo import NeoETL

jobs = [MongoETL(), NeoETL()]

if __name__ == '__main__':
    for job in jobs:
        job.run()
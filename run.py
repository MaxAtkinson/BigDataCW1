from etl.etl import ETL
from etl.mongo import MongoTL
from etl.neo import NeoTL

if __name__ == '__main__':
    ETL([MongoTL(), NeoTL()]).run()
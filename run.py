from etl.mongo import MongoETL
from etl.neo import NeoETL

if __name__ == '__main__':
    for task in [NeoETL()]:
        task.run()
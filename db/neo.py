from neo4j import GraphDatabase

''' Connects to Neo4j. Drops chinook db so it can be run multiple times. Exports db session. '''
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
db = driver.session
with db() as session:
    session.run('''
    MATCH (n) 
    DETACH DELETE n;''')
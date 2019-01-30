from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
db = driver.session
with db() as session:
    session.run('''
    MATCH (n) 
    DETACH DELETE n;
    ''')
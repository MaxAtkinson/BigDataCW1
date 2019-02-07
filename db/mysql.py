import mysql.connector

''' Connects to MySQL. Exports a database cursor for querying. '''
db = mysql.connector.connect(
	host='localhost',
	user='root',
	passwd='nosql123',
	database='Chinook')
cursor = db.cursor()

import mysql.connector

db = mysql.connector.connect(
	host='localhost',
	user='root',
	passwd='nosql123',
	database='Chinook')

cursor = db.cursor()
cursor.execute('''SET SESSION group_concat_max_len = 25000;''')

import pymysql.cursors
import csv

DB_HOST = "127.0.0.1"
DB_USER = "homestead"
DB_PASS = "secret"
DB_PORT = 33060
DB_DATABASE = "hva_bigdata_indv_assignment"
def db_connect(host, user, passwd, db, port):
	connection = pymysql.connect(
			host=host,
			user=user,
			password=passwd,
			db=db,
			port=port,
			charset='utf8mb4',
			cursorclass=pymysql.cursors.DictCursor
		)

	return connection

reader = csv.reader(open('data/links.csv', 'rb'))
headers = reader.next()
db_connection = db_connect(DB_HOST, DB_USER, DB_PASS, DB_DATABASE, DB_PORT)

for line in reader:
	print "1: " + line[0]
	print "2: " + line[1]
	print "3: " + line[2]
	try:
		cursor = db_connection.cursor()
		print("Inserting row. TMDB ID: " + line[1])
		sql = "INSERT INTO `movielens_tmdb_imdb` (`id`, `tmdb_id`, `imdb_id`) VALUES (%d, %d, %d);"
		cursor.execute( sql, ( int( line[0] ), int( line[1] ), int( line[2] ) ) )
		cursor.commit()
	finally:
		print("Inserted ID: " + line[0])

db_connection.close()
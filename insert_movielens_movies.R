library(jsonlite)
SECRETS_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/secrets.json"
DATA_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/data/movies.csv"

secrets = read_json(path = SECRETS_PATH)

DB_HOST <- secrets$db_host
DB_USER <- secrets$db_user
DB_PASSWD <- secrets$db_pass
DB_DATABASE <- secrets$db_database
DB_PORT <- secrets$db_port

dbCon <- dbConnect(
  MySQL(),
  user = DB_USER,
  password = DB_PASSWD,
  host = DB_HOST,
  port = DB_PORT,
  dbname = DB_DATABASE
)

data <- read.csv2(file = DATA_PATH, header = FALSE, sep = ",")
dbWriteTable(conn = dbCon, name='movielens_movies', value= data, overwrite=TRUE, row.names=FALSE)
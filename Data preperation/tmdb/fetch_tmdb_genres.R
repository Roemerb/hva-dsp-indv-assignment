library(jsonlite)
library(RMySQL)

SECRETS_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/secrets.json"

secrets = read_json(path = SECRETS_PATH)
tmdbApiKey = secrets$tmdb_api_key
genres = fromJSON(paste("https://api.themoviedb.org/3/genre/movie/list?api_key=",tmdbApiKey,"&language=en-US", sep=""))
genres <- genres$genres
genres <- data.frame(genres)

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

dbWriteTable(conn = dbCon, name='tmdb_genres', value= genres, overwrite=TRUE, row.names=FALSE)
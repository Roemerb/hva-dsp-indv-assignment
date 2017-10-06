library(RMySQL)
library(jsonlite)

SECRETS_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/secrets.json"
secrets <- read_json(path = SECRETS_PATH)


DB_HOST <- secrets$db_host
DB_USER <- secrets$db_user
DB_PASSWD <- secrets$db_pass
DB_DATABASE <- secrets$db_database
DB_PORT <- secrets$db_port
DATA_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/data/links.csv"

data <- read.csv2(file=DATA_PATH, sep = ",")

dbCon <- dbConnect(
  MySQL(),
  user = DB_USER,
  password = DB_PASSWD,
  host = DB_HOST,
  port = DB_PORT,
  dbname = DB_DATABASE
)

for (row in rownames(data)) {
  if (is.na(data[row, "movieId"]))
  {
    next;
  }
  if (is.na(data[row, "tmdbId"]))
  {
    next;
  }
  if (is.na(data[row, "imdbId"]))
  {
    next;
  }
  
  dbSendQuery(dbCon, paste(
    "INSERT INTO `movielens_tmdb_imdb` (`id`, `tmdb_id`, `imdb_id`) VALUES (", data[row, "movieId"], ", ", data[row, "tmdbId"], ", ", data[row, "imdbId"], ");"
    )
  )
  print(paste("Inserted movie ", data[row, "movieId"]))
}
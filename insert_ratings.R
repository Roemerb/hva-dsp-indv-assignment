library(jsonlite)
library(plyr)
library(ggplot2)
library(RMySQL)

SECRETS_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/secrets.json"
DATA_PATH = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/data/ratings.csv"

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

data <- read.csv2(file = DATA_PATH, header = TRUE, sep = ",", nrows=1000000, stringsAsFactors = FALSE)

# Rating data is loaded as factor instead of numeric. We'll convert it to numeric so we can aggregate
data$rating = as.numeric(data$rating)

meanRatings <- aggregate(rating ~ movieId, data, mean)
ratingCounts <- aggregate(rating ~ movieId, data, length)
combined <- cbind(meanRatings, ratingCounts)
# Rename columns
colnames(combined)[2] <- "mean_rating"
colnames(combined)[3] <- "movieIdDuplicate"
colnames(combined)[4] <- "rating_count"
combined$movieIdDuplicate <- NULL

# Double mean ratings as movielens goes from 1 to 5
ratings <- list(combined$mean_rating)
ratings <- lapply(ratings, function(x) {x <- x * 2})
ratings <- as.data.frame(ratings)
# Assign to combined data frame
combined$mean_rating <- unlist(ratings)
# Rename the suddenly weirdly named column
colnames(combined)[2] <- "mean_rating"


# Insert into the database
dbWriteTable(conn = dbCon, name='movielens_ratings', value= combined, overwrite=TRUE, row.names=FALSE)
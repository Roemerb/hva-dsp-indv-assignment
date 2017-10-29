library(RMySQL)
library(dplyr)

movies <- dbReadTable(conn = dbCon, name = 'tmdb_movies')

# In some cases where the rate limit was hit, the response code from
# the API was still 200, and therefore the result was inserted in the
# database. These records can easily be identified as the `id` field is
# 0. We'll remove these obvious rows first.
dbSendQuery(conn = dbCon, 'DELETE FROM `tmdb_movies` WHERE `id` = 0;')

# After inspecting the data, I found out that some duplicate movies
# still made it into the data. We'll use dplyr::distinct to only
# keep the rows where id is unique. We can use the resulting data
# frame and dbWriteTable to save the new data to the database.
movies <- distinct(.data=movies, id, .keep_all=TRUE)
dbWriteTable(conn = dbCon, name = 'tmdb_movies2', value = movies, overwrite=TRUE)
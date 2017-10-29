library(graphics)
library(lubridate)

tmdb <- dbReadTable(conn=dbCon, name ='tmdb_movies2')
movielensRes <- dbSendQuery(conn = dbCon, 'SELECT 
                            movielens_ratings.mean_rating, 
                            movielens_ratings.rating_count, 
                            movielens_movies.title 
                            FROM movielens_ratings 
                            RIGHT JOIN movielens_movies 
                            ON movielens_ratings.movieId=movielens_movies.movieId;')
movielens <- fetch(movielensRes, n=-1)
dbClearResult(movielensRes)

tmdb$release_date <- as.Date(tmdb$release_date)
tmdb$vote_count <- as.numeric(tmdb$vote_count)
tmdb$release_year <- year(tmdb$release_date)

movielens$release_year <- year(as.Date(paste(str_sub(movielens$title, -5, -2), '-01-01', sep='')))

tmdb_line_points <- aggregate(vote_count ~ release_year, tmdb, length)
movielens_line_points <- aggregate(rating_count ~ release_year, movielens, length)

line_xrange <- range(line_points$release_year)
line_yrange <- range(line_points$vote_average)

plot(c(1891, 2016), c(1, 1093), type="n", xlab="Years", ylab="Number of ratings")
title(main='Number of ratings over time (years)', sub='The Movie Database vs. MovieLens')
grid(col='black')
legend("topleft", legend=c("The Movie Database", "MovieLens"), col=c("Blue", "Red"), lty=1:1, cex=0.8)

lines(tmdb_line_points$release_year, tmdb_line_points$vote_count, type ="l", col='#4286f4')
lines(movielens_line_points$release_year, movielens_line_points$rating_count, 'type' = 'l', col='#ff0000')
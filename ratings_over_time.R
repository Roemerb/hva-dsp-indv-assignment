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
tmdb$vote_average <- as.numeric(tmdb$vote_average)
tmdb$release_year <- year(tmdb$release_date)

movielens$release_year <- year(as.Date(paste(str_sub(movielens$title, -5, -2), '-01-01', sep='')))

tmdb_line_points <- aggregate(vote_average ~ release_year, tmdb, mean)
movielens_line_points <- aggregate(mean_rating ~ release_year, movielens, mean)

line_xrange <- range(line_points$release_year)
line_yrange <- range(line_points$vote_average)

plot(line_xrange, c(0, 10), type="n", xlab="Years", ylab="Average score")
title(main='Mean review score over time (years)', sub='The Movie Database vs. MovieLens')
grid(col='black')
legend("topright", legend=c("The Movie Database", "MovieLens"), col=c("Blue", "Red"), lty=1:2, cex=0.8)

lines(tmdb_line_points$release_year, line_points$vote_average, type ="l", col='#4286f4')
lines(movielens_line_points$release_year, movielens_line_points$mean_rating, 'type' = 'l', col='#ff0000')
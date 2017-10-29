library(RMySQL)

tmdb <- dbReadTable(conn = dbCon, 'tmdb_movies')
movielens <- dbReadTable(conn = dbCon, 'movielens_ratings')

tmdb_distribution <- table(round(as.numeric(tmdb$vote_average)))
movielens_distribution <- table(round(movielens$mean_rating))

n <- intersect(names(tmdb_distribution), names(movielens_distribution))
combined <- tmdb_distribution[n] + movielens_distribution[n]

barplot(combined, main="Rating distribution", ylab = "Number of occurences", xlab = "Rating")
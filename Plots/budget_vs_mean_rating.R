options(scipen = 999)
tmdb <- dbReadTable(conn = dbCon, 'tmdb_movies')as.?
tmdb$vote_average <- as.numeric(tmdb$vote_average)

line_points <- aggregate(vote_average ~ budget, subset(tmdb, budget < 1000000), mean)
# Order by budget
line_points <- line_points[order(line_points$budget)]
line_xrange <- range(line_points$budget)
line_yrange <- range(line_points$vote_average)

plot(line_xrange, line_yrange, type="n", xlab="Budget", ylab="Mean rating")
title(main="Budget vs. mean rating", sub="Does a higher budget mean higher ratings?")
grid(col="black")
lines(line_points$budget, line_points$vote_average, type="l", col='#4286f4')

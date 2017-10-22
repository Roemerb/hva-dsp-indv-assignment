library(httr)

movies <- dbReadTable(con = dbCon, name = 'movielens_tmdb_imdb')
maxTries <- 5
rateLimitRemaining <- 40
rateLimitReset <- 0

fetchMovie <- function(movieId, tries) {
  if (missing(tries)) {
    tries <- 1
  }
  if (tries > maxTries) {
    return(NULL)
  }
  
  if (canCallAPI()) {
    print(paste("Fetching movie ", movieId, " Try: ", tries, sep=""))
    response <- httr::GET(url = getUrl(movieId))
    
    if (response$status_code == 200) {
      return(response)
    } else if (response$status_code == 500) {
      print(paste("Fetching movie ", movieId, " failed. Status code 500. Retrying"))
      tries <- tries + 1
      fetchMovie(movieId, tries)
    } else if (response$status_code == 429) {
      print("Hit rate limit hard. Invoking rate limiter")
      invokeRateLimit(movieId, tries)
    }
    setRateLimit(response$headers$`x-ratelimit-remaining`, response$headers$headers$`x-ratelimit-reset`)
  } else {
    invokeRateLimit(movieId, tries)
  }
  
  return(response)
}

canCallAPI <- function() {
  return(rateLimitRemaining != 0)
}

setRateLimit <- function(remaining, reset) {
  rateLimitRemaining = remaining
  rateLimitReset = as.numeric(reset)
}

invokeRateLimit <- function(movieId, tries) {
  rlReset <- as.POSIXct(rateLimitReset, origin="1970-01-01", tz=Sys.timezone())
  diff <- ceiling(as.numeric(rlReset - Sys.time()))
  print(paste("Rate limit hit. Sleeping for ", diff, " seconds"))
  Sys.sleep(diff)
  
  print("Rate limit over. Retrying.")
  tries <- tries + 1
  fetchMovie(movieId, tries)
}

getUrl <- function(movieId) {
  return(paste("https://api.themoviedb.org/3/movie/", movieId, "?api_key=", secrets$tmdb_api_key, "&language=en-US", sep = ""))
}

saveGenres <- function(movieId, genres) {
  for (genre in rownames(genres)) {
    dbSendQuery(dbCon, paste("INSERT INTO `tmdb_genre_movie` VALUES (", movieId, ", ", genres[genre, "id"], ");"))
  }
  print(paste("Saved genres for movie ", movieId))
}

saveMovie <- function(movie) {
  print(paste("Saving movie ", movie$id, " title: ", movie$original_title))
  query <- paste(
    'INSERT INTO tmdb_movies (
    `tmdb_id`, 
    `adult`, 
    `backdrop_path`, 
    `budget`, 
    `homepage`, 
    `original_language`, 
    `overview`, 
    `popularity`, 
    `poster_path`, 
    `release_date`, 
    `revenue`, 
    `runtime`, 
    `status`, 
    `tagline`, 
    `title`, 
    `video`, 
    `vote_average`, 
    `vote_count`
  ) VALUES (
    `tmdb_id` = ', movie$id, ',
    `adult` = ', movie$adult, ',
    `backdrop_path` = "', movie$backdrop_path, '",
    `budget` = ', movie$budget, ',
    `homepage` = "', movie$homepage, '",
    `original_language` = "', movie$original_language, '",
    `overview` = "', movie$overview, '",
    `popularity` = ', movie$popularity, ',
    `poster_path` = "', movie$poster_path, '",
    `release_date` = "', movie$release_date, '",
    `revenue` = ', movie$revenue, ',
    `runtime` = ', movie$runtime, ',
    `status` = "', movie$status, '",
    `tagline` = "', movie$tagline, '",
    `title` = "', movie$title, '",
    `video` = "', movie$video, '",
    `vote_average` = ', movie$vote_average, ',
    `vote_count` = ', movie$vote_count, 
    ');', sep="")
  
  dbSendQuery(dbCon, statement = query)
}

for (movie in rownames(movies)) {
  res <- fetchMovie(movies[movie, "tmdb_id"])
  json <- fromJSON(content(res, "text"))
  
  saveGenres(movieId = movies[movie, "tmdb_id"], genres = json$genres)
  saveMovie(json)
}

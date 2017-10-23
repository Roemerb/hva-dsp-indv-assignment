library(httr)
library(RMySQL)
library(jsonlite)

movies <- dbReadTable(con = dbCon, name = 'movielens_tmdb_imdb')
fetchedMovies <- setupFetchedMovies()
maxTries <- 10
rateLimitRemaining <- 40
rateLimitReset <- 0
offsetIfFailed = 9999;

setupFetchedMovies <- function() {
  movie <- fetchMovie(2)
  movie <- parseResponse(movie)

  return(as.data.frame(x=movie, stringsAsFactors = FALSE))
}

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
      print(paste("Hit rate limit hard. Sleeping for ", response$headers$`retry-after`, " seconds", sep=""))
      Sys.sleep(as.numeric(response$headers$`retry-after`))
      fetchMovie(movieId, tries + 1)
    } else if (response$status_code == 401) {
      print(paste("No movie with ID ", movieId, " could be found. Skipping."))
      return(NULL)
    }
  } else {
    print(paste("Hit rate limit. Sleeping for ", response$headers$`retry-after`, " seconds", sep=""))
    Sys.sleep(response$headers$`retry_after`)
    fetchMovie(movieId, tries + 1)
  }
  
  return(response)
}

canCallAPI <- function() {
  return(rateLimitRemaining != 0)
}

setRateLimit <- function(remaining, reset) {
  rateLimitRemaining <- remaining
  rateLimitReset <- as.numeric(reset)
}

parseResponse <- function(response) {
  response <- content(response, "text")
  movie <- fromJSON(response)
  movie$belongs_to_collection <- NULL
  movie$genres <- NULL
  movie$imdb_id <- NULL
  movie$production_companies <- NULL
  movie$production_countries <- NULL
  movie$spoken_languages <- NULL
  
  if(is.null(movie$adult)) {
    movie$adult <- FALSE
  }
  if (is.null(movie$backdrop_path)) {
    movie$backdrop_path <- ""
  }
  if (is.null(movie$budget)) {
    movie$budget <- 0
  }
  if (is.null(movie$homepage)) {
    movie$homepage <- ""
  }
  if (is.null(movie$id)) {
    movie$id <- 0
  }
  if (is.null(movie$original_language)) {
    movie$original_language <- ""
  }
  if (is.null(movie$original_title)) {
    movie$original_title <- ""
  }
  if (is.null(movie$overview)) {
    movie$overview <- ""
  }
  if (is.null(movie$popularity)) {
    movie$popularity <- 0
  }
  if (is.null(movie$poster_path)) {
    movie$poster_path <- ""
  }
  if (is.null(movie$release_date)) {
    movie$release_date <- "1970-01-01"
  }
  if (is.null(movie$revenue)) {
    movie$revenue <- 0
  }
  if (is.null(movie$runtime)) {
    movie$runtime <- 0
  }
  if (is.null(movie$status)) {
    movie$status <- ""
  }
  if (is.null(movie$tagline)) {
    movie$tagline <- ""
  }
  if (is.null(movie$title)) {
    movie$title <- ""
  }
  if (is.null(movie$video)) {
    movie$video <- ""
  }
  if (is.null(movie$vote_average)) {
    movie$vote_average <- 0
  }
  if (is.null(movie$vote_count)) {
    movie$vote_count <- 0
  }
  
  return(as.data.frame(movie, stringsAsFactors = FALSE))
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
  print(query)
  dbSendQuery(dbCon, statement = query)
}

for (movie in rownames(movies)) {
  if (as.numeric(movie) <= offsetIfFailed) {
    print(paste(movie, " smaller than offset. Next", sep=""))
    next
  }
  if (length(which(fetchedMovies$id == movies[movie, "tmdb_id"])) > 0) {
    print(paste("Already saw movie with id ", movies[movie,"tmdb_id"], ", skipping...", sep=""))
    next
  }
  res <- fetchMovie(movies[movie, "tmdb_id"])
  if (is.null(res)) {
    next
  }
  res <- parseResponse(res)
  fetchedMovies[nrow(fetchedMovies)+1,] <- res
  #json <- fromJSON(content(res, "text"))
  
  #fetchedMovies <- merge.data.frame(x = fetchedMovies, y=as.data.frame(json))
  #saveGenres(movieId = movies[movie, "tmdb_id"], genres = json$genres)
  #saveMovie(json)
}

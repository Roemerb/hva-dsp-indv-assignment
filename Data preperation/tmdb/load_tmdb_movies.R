# load_tmdb_movies.R
# @author Roemer Bakker
#
# This script will use the movielens 'links' to build a database of movie
# data from the website themoviedb.org. The script uses the tmdb HTTP API
# download metadata about movies by their ID and store them in a MySQL
# database.

library(httr)
library(RMySQL)
library(jsonlite)

# First of all, we'll use RMySQL to read the 'movielens_tmdb_imdb' table.
# This table is created with 'insert_links.R' and stores the contents
# of the links.csv file in a MySQL database.
movies <- dbReadTable(con = dbCon, name = 'movielens_tmdb_imdb')
# fetchedMovies is the data table that will be used to store the data
# from the tmdb API. We'll use the function setupFetchedMovies to
# make sure the data table has the right format
fetchedMovies <- setupFetchedMovies()
# Several things can go wrong when calling an HTTP API. By default,
# when an error occurs, the movie will be tried to be fetched again
# maxTries defines the maximum number of times the request will be
# tried.
maxTries <- 10
# When executing over 27.000 API requests, several things can go wrong.
# For instance, the network connection may go down. In this case, this
# script will crash and will have to be restarted. It would be a shame
# if all 27k requests have to be retried, this would also increase the
# likelyhood a fatal error occurs again. offsetIfFailed indicates where
# the script crashed last time, and these movies will be skipped.
offsetIfFailed <- 0;
# Indicates if we want to save the genres tmdb provides to the
# database as well
saveGenres <- TRUE
# Indicates if we want to store the movies one by one after the response
# has been received, or if we'll save it all at once at the end.
saveMoviesRightAway <- FALSE
# Indicates if debug info is printed
debugMode <- FALSE

# setupFetchedMovies - function
#
# Sets up the fetchedMovies dataframe.
#
# @return data.frame
setupFetchedMovies <- function() {
  # Fetch a random movie
  movie <- fetchMovie(2)
  # Parse the response
  movie <- parseResponse(movie)

  # Return result as dataframe. Make sure strings aren't converted
  # to factors
  return(as.data.frame(x=movie, stringsAsFactors = FALSE))
}

# fetchMovie - function
#
# Uses the tmdb API to fetch information about a movie.
#
# @param numerical movieId The ID of the movie to be fetched
# @param numerical tries Indicates which try this is
# @return httr::response
fetchMovie <- function(movieId, tries) {
  # If tries is not provided, default to 1
  if (missing(tries)) {
    tries <- 1
  }
  # If tries exceets the maximum allowed number of tries, exit function
  # by returning NULL
  if (tries > maxTries) {
    return(NULL)
  }
  
  # Print to console that we're going to fetch a movie for the n'th time
  print(paste("Fetching movie ", movieId, " Try: ", tries, sep=""))
  # Call the API using httr. Get formatted URL from getUrl
  response <- httr::GET(url = getUrl(movieId))
  
  if (response$status_code == 200) {
    # If the response code is 200, all went fine. Return the response
    return(response)
  } else if (response$status_code == 500) {
    # If the response code is 500, something went wrong on the side of tmdb
    # we'll retry the request using tries +1 to make sure we won't end up in
    # an infinite loop
    if (debugMode) {
      print(paste("Fetching movie ", movieId, " failed. Status code 500. Retrying"))
    }
    tries <- tries + 1
    fetchMovie(movieId, tries)
  } else if (response$status_code == 429) {
    # If the status is 429, we've hit the rate limiter. The response will
    # contain a header called 'retry-after'. The value of this header is the amount
    # of seconds we'll have to wait before the rate limit is reset. We'll add an extra
    # second to this amount as the value of the header will become '0' which means we'll
    # still have to wait for another second.
    if (debugMode) {
      print(paste("Hit rate limit. Sleeping for ", response$headers$`retry-after`, " seconds", sep=""))
    }
    Sys.sleep(as.numeric(response$headers$`retry-after`) + 1)
    fetchMovie(movieId, tries + 1)
  } else if (response$status_code == 401) {
    # If the status is 401, this means that no movie with this ID exists in the tmdb
    # database. We'll return NULL
    if (debugMode) {
      print(paste("No movie with ID ", movieId, " could be found. Skipping."))
    }
    return(NULL)
  }
  
  # If we didn't see any of the status codes above, something unknown went wrong.
  # We'll return null
  return(NULL)
}

# parseResponse - function
#
# Get's the content from the response and parses it to JSON.
# Removes data we don't need and defaults empty values.
#
# @param httr::response response The raw response from the API
# @return data.frame
parseResponse <- function(response) {
  # Get response and parse the JSON to a data.frame
  response <- content(response, "text")
  movie <- fromJSON(response)
  
  # Remove arrays and objects to make sure we have a flat data frame
  movie$belongs_to_collection <- NULL
  movie$genres <- NULL
  movie$imdb_id <- NULL
  movie$production_companies <- NULL
  movie$production_countries <- NULL
  movie$spoken_languages <- NULL
  
  # Set empty values to defaults.
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
  
  # Return result as data frame. Make sure strings are not converted
  # to factors
  return(as.data.frame(movie, stringsAsFactors = FALSE))
}

# getURL - function
#
# Takes default tmdb movie endpoint and adds API key from secrets and the movie ID
#
# @param numerical movieId The ID of the movie to be fetched
# @return character
getUrl <- function(movieId) {
  return(paste("https://api.themoviedb.org/3/movie/", movieId, "?api_key=", secrets$tmdb_api_key, "&language=en-US", sep = ""))
}

# saveGenres - function
#
# If saveGenres is enabled, this function will persist the genres provided in a response
# to the database
#
# @param numerical movieId The ID of the movie
# @param data.frame genres The genres from the response
saveGenres <- function(movieId, genres) {
  # Simply iterate over the genres and use an insert query to store in the database
  for (genre in rownames(genres)) {
    dbSendQuery(dbCon, paste("INSERT INTO `tmdb_genre_movie` VALUES (", movieId, ", ", genres[genre, "id"], ");"))
  }
  if (debugMode) {
    print(paste("Saved genres for movie ", movieId))
  }
}

# saveMovie - function
#
# Will store a single movie in the database using a simple insert query.
#
# @param data.frame movie The parsed movie data from the API
saveMovie <- function(movie) {
  # Print we're going to save the movie
  print(paste("Saving movie ", movie$id, " title: ", movie$original_title))
  # Define the query
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
  if (debugMode) {
    # Print query to the console for debugging
    print(query)
  }
  # Sent query to the database
  dbSendQuery(dbCon, statement = query)
}

# Main control structure of the script. Iterates of the data in the links table
for (movie in rownames(movies)) {
  # If an offset is set, skip 'till movie > offsetIfFailed
  if (as.numeric(movie) <= offsetIfFailed) {
    if (debugMode) {
      print(paste(movie, " smaller than offset. Next", sep=""))
    }
    next
  }
  # Make sure there won't be any duplicates in the fetchedMovies data.table by using
  # `which` to search the table for the movie ID. Skips this movie if it has already been
  # seen
  if (length(which(fetchedMovies$id == movies[movie, "tmdb_id"])) > 0) {
    if (debugMode) {
      print(paste("Already saw movie with id ", movies[movie,"tmdb_id"], ", skipping...", sep=""))
    }
    next
  }
  # Use fetchMovie to get data from the API
  res <- fetchMovie(movies[movie, "tmdb_id"])
  # If the response is null, something went wrong while calling the API. We'll continue
  # with the next movie
  if (is.null(res)) {
    next
  }
  # Parse the response
  res <- parseResponse(res)
  # Insert the parsed response into the fetchedMovies data.frame
  fetchedMovies[nrow(fetchedMovies)+1,] <- res
  
  # If saveGenres is enabled, use saveGenres to store genres to the database
  if (saveGenres) {
    saveGenres(movieId = movies[movie, "tmdb_id"], genres = json$genres)
  }
  # If saveMoviesRightAway is enabled, save the movie to the database right away
  if (saveMoviesRightAway) {
    saveMovie(json)
  }
}

# If saveMoviesRightAway is not enabled, use dbWriteTable to save the entire fetchedMovies
# data.frame to the database
if (!saveMoviesRightAway) {
  dbWriteTable(conn = dbCon, name = 'tmdb_movies', value = fetchedMovies, overwrite = TRUE)
}
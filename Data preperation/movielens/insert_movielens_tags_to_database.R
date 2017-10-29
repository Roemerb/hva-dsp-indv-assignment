data <- read.csv2(
  file = "/Users/roemerbakker/ownCloud/HvA/Data Processing And Storage/Individual Assignment/data/tags.csv", 
  stringsAsFactors = FALSE,
  sep = ',')

dbWriteTable(conn = dbCon, 'movielens_tags', data, overwrite = TRUE, row.names=FALSE)

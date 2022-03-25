rm(list = ls());cat("\014")  # clear console and environment

# functions and directories
paths <- list()
paths[["current"]] <- paste0(dirname(getwd()), "/")
source(paste0(paths$current, "R/main.R"))

ParseDirectors <- function(paths, con){
  
  # start duckdb connection
  duckdb = dbConnect(con$connection, dbdir = con$directory, read_only=FALSE)
  
  # extract directors from duckdb
  directors = as.data.table(dbGetQuery(duckdb, "SELECT movie, director FROM directing"))
  
  # select tconst columns
  train = as.data.table(dbGetQuery(duckdb, "SELECT tconst, numVotes FROM train"))
  
  train = merge(train, directors, by.x = "tconst", by.y = "movie", all.x = T)
  
  # TARGET ENCODING
  # First infer the most frequent value for numvotes
  train = TargetEncoding(train, "director", "numVotes")
  
  train = train[, .(tconst, director_mean = value_mean)]
  
  cat("writing to directors table")
  dbWriteTable(duckdb, "directors", train, overwrite = TRUE)  
  dbDisconnect(duckdb, shutdown = T)
  
}

ParseDirectors(paths, con)


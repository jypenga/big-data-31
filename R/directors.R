rm(list = ls());cat("\014")  # clear console and environment

# functions and directories
paths <- list()
paths[["current"]] <- paste0(getwd(), "/")
source(paste0(paths$current, "R/main.R"))

ParseDirectors <- function(paths, con, i){
  
  # start duckdb connection
  duckdb = dbConnect(con$connection, dbdir = con$directory, read_only=TRUE)
  
  # extract directors from duckdb
  directors = as.data.table(dbGetQuery(duckdb, "SELECT movie, director FROM directing"))
  
  # select tconst columns
  train = as.data.table(dbGetQuery(duckdb, paste0("SELECT tconst, numVotes FROM ", i)))
  
  train = merge(train, directors, by.x = "tconst", by.y = "movie", all.x = T)
  
  # TARGET ENCODING
  # First infer the most frequent value for numvotes
  train = TargetEncoding(train, "director", "numVotes")
  
  train = train[, .(tconst, directors = value_mean)]
  
  dbDisconnect(duckdb, shutdown = T)
  
  write.csv(train, paste0(paths$data, i, "_directors.csv"), row.names=FALSE)
}

for(i in tables$directors){
  ParseDirectors(paths, con, i)
}


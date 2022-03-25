rm(list = ls());cat("\014")  # clear console and environment

# functions and directories
paths <- list()
paths[["current"]] <- paste0(dirname(getwd()), "/")
source(paste0(paths$current, "R/main.R"))

ParseDirectors <- function(paths, con, i){
  
  # start duckdb connection
  duckdb = dbConnect(con$connection, dbdir = con$directory, read_only=FALSE)
  
  # extract directors from duckdb
  directors = as.data.table(dbGetQuery(duckdb, "SELECT movie, director FROM directing"))
  
  # select tconst columns
  train = as.data.table(dbGetQuery(duckdb, paste0("SELECT tconst, numVotes FROM ", i)))
  
  # close duckdb connection
  dbDisconnect(duckdb, shutdown = T)
  
  train = merge(train, directors, by.x = "tconst", by.y = "movie", all.x = T)
  
  # TARGET ENCODING
  # First infer the most frequent value for numvotes
  train = TargetEncoding(train, "director", "numVotes")
  
  train = train[, .(tconst, director_mean = value_mean)]
  
  cat("writing to directors table")
  #dbWriteTable(duckdb, paste0(i, "_directors"), train, overwrite = TRUE)  
  write.csv(train, paste0(paths$data, i, "_directors.csv"))
  
}


for(i in tables$directors){
  ParseDirectors(paths, con, i)
}



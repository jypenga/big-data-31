rm(list = ls());cat("\014")  # clear console and environment

# functions
paths <- list()
paths[["current"]] <- paste0(getwd(), "/")
source(paste0(paths$current, "R/main.R"))

ParseWriters <- function(paths, con, i){
  
  # start duckdb connection
  duckdb = dbConnect(con$connection, dbdir = con$directory, read_only=FALSE)
  
  # extract writers from duckdb
  writers = as.data.table(dbGetQuery(duckdb, "SELECT movie, writer FROM writing"))
  
  train = as.data.table(dbGetQuery(duckdb, paste0("SELECT tconst, numVotes FROM ", i)))
  
  writers = merge(writers, train, by.x = "movie", by.y = "tconst", all.x = T)
  
  # TARGET ENCODING
  writers = TargetEncoding(writers, "writer", "numVotes")
  
  # set incremental index of writers to be used in dcast
  setDT(writers)[, Index := seq_len(.N), by = movie]
  
  # go from long to wide format
  writers = dcast(writers, movie ~ paste0("writer", Index), value.var = "value_mean")
  
  # select tconst columns
  train = merge(train[, .(tconst)], writers, by.x = "tconst", by.y = "movie", all.x = T)
  
  dbWriteTable(duckdb, paste0(i, "_writers"), train, overwrite = TRUE)
  dbDisconnect(duckdb, shutdown = T)

  cat(paste0('\n', i, '_writers'))
}

for(i in tables$writers){
  ParseWriters(paths, con, i)
}

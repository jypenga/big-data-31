rm(list = ls());cat("\014")  # clear console and environment

# paths
paths <- list()
paths[["current"]] <- paste0(dirname(getwd()), "/")
paths[["db"]] <- paste0(paths$current, "db/")
paths[["data"]] <- paste0(paths$current, "dump/")

# libraries
library("DBI")
library("data.table")
library("jsonlite")
library("mltools")
library("stringr")

# connect database
con = list(connection = duckdb::duckdb(), directory = paste0(paths$data, "db.duckdb"))

TargetEncoding <- function(dt, cols, values){
  
  # TARGET ENCODING
  # First infer the most frequent value for numvotes
  setDT(dt)[, N:=.N, values][N==max(N)][, N:=NULL]
  most_frequent <- max(dt[N != max(N)]$N)
  most_frequent_votes = first(dt[N == most_frequent]$numVotes)
  
  # set the missing values to this value
  dt = dt[is.na(dt[[values]]), c(values) := most_frequent_votes]

  #calculate mean numvotes per director
  dt = dt[, value_mean := lapply(.SD, mean), .SDcols = c(values), by= c(cols)]
  
  return(dt)
  
}

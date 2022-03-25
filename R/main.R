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
  # First infer the mean value for numvotes
  mean_votes = colMeans(dt[!is.na(dt[[values]]), ..values])
  
  # set the missing values to this value
  dt = dt[is.na(dt[[values]]), c(values) := mean_votes]
  
  #calculate mean numvotes per director
  dt = dt[!is.na(dt[[values]]), value_mean := lapply(.SD, mean), .SDcols = c(values), by= c(cols)]
  
  return(dt)
  
}

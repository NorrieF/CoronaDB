library(RSQLite)
library(readr)
library(foreach)

conn <- dbConnect(RSQLite::SQLite(), "CoronaDB.db")

data <- read_csv("C:/Users/Norrie/Desktop/statenames.csv")
dbWriteTable(conn, "statenames", data)
file <- read_csv("C:/Users/Norrie/Desktop/acronyms.csv")
dbWriteTable(conn, "acronyms", file)

dbDisconnect(conn)

library(RSQLite)
library(readr)
library(foreach)

conn <- dbConnect(RSQLite::SQLite(), "CoronaDB.db")

dbRemoveTable(conn, "statenames")
dbRemoveTable(conn, "acronyms")

data <- read_csv("C:/Users/Norrie/Desktop/canadastate.csv")
dbWriteTable(conn, "statenames", data)
file <- read_csv("C:/Users/Norrie/Desktop/acrocanada.csv")
dbWriteTable(conn, "acronyms", file)

dbDisconnect(conn)

library(RSQLite)

conn <- dbConnect(RSQLite::SQLite(), "CoronaDB.db")

dbGetQuery(conn, "SELECT * FROM [03-05-2020]")
dbDisconnect(conn)

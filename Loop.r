library(RSQLite)
library(readr)
library(foreach)

conn <- dbConnect(RSQLite::SQLite(), "CoronaDB.db")

dates <- seq(as.Date("01-22-2020", "%m-%d-%Y"), as.Date("04-15-2020", "%m-%d-%Y"), by = 1)
dates <- format(dates, format = "%m-%d-%Y")
names <- as.character(dates)

foreach(d = dates, i = names) %do% {
    file = paste0("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/", d, ".csv")
    data <- read_csv(url(file))
    dbWriteTable(conn, i, data)
    }

dbDisconnect(conn)

dbGetQuery(conn, "SELECT * FROM [03-09-2020]")

dbDisconnect(conn)

library(RSQLite)
library(readr)
library(foreach)

conn <- dbConnect(RSQLite::SQLite(), "CoronaDB.db")



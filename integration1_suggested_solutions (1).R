library(RPostgres)
library(DBI)
library(tidyverse)
library(httr2)
library(lubridate)

# Exercise 3 --------------------------------------------------------------
# Put the credentials in this script
# Never push credentials to git!! --> use .gitignore on .credentials.R
source(".credentials.R")
# Function to send queries to Postgres
source("psql_queries.R")
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg1;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table intg1.students (
                	 student_id serial primary key,
	                 student_name varchar(255),
	                 department_code int
                   );")
# Write rows in the new table
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "insert into intg1.students
	values (default, 'Martin', '1')
		  ,(default, 'Charlotte', '4');")
# Fetch the data
psql_select(cred = cred_psql_docker, 
            query_string = "select * from intg1.students;")

# If you wish you can remove the intg1 schema again
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA intg1 cascade;")
# Exercise 5 --------------------------------------------------------------
# Investigate which symbols we can search for
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("keywords" = "Tesla",
                "function" = "SYMBOL_SEARCH",
                "datatype" = "json") %>%
  req_headers('X-RapidAPI-Key' = 'INSERT OWN API KEY',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
symbols <- resp %>%
  resp_body_json()
symbols$bestMatches[[1]]
symbols$bestMatches[[2]]

# Extract  ------------------------------------------
# Extract data from Alpha Vantage
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "60min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "TSLA",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = 'INSERT OWN API KEY',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()


# Exercise 6 --------------------------------------------------------------
# TRANSFORM timestamp to UTC time 
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (60min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")
# Prepare data.frame to hold results
df <- tibble(timestamp = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)
# TRANSFORM data into a data.frame
for (i in 1:nrow(df)) {
  df[i,-1] <- as.data.frame(dat$`Time Series (60min)`[[i]])
}

# Create table in Postgres ------------------------------------------------
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg2;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table intg2.prices2 (
	id serial primary key,
	timestamp timestamp(0) without time zone,
	close numeric(30,4),
	volume numeric(30,4));")

# LOAD price data 
psql_append_df(cred = cred_psql_docker,
               schema_name = "intg2",
               tab_name = "prices2",
               df = df[,c("timestamp", "close", "volume")])

# Check results
# Check that we can fetch the data again
psql_select(cred = cred_psql_docker, 
            query_string = 
              "select * from intg2.prices2")
# If you wish, your can delete the schema (all the price data) from Postgres 
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA intg2 cascade;")

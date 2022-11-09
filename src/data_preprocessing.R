# Libraries ----
library(tidyverse)
library(lubridate)
library(RPostgres)

# Set basic parameters

## Define locale to ENG ----
Sys.setlocale("LC_TIME", "English")

## Define connections to databases ----

# PostgreSQL cloud database connection
con_cloud <- dbConnect(RPostgres::Postgres(),
                       dbname = Sys.getenv("TRAIN_DBNAME"),
                       host = Sys.getenv("TRAIN_HOST"), 
                       port = Sys.getenv("TRAIN_PORT"),
                       user = Sys.getenv("TRAIN_USER"), 
                       password = Sys.getenv("TRAIN_PWD"))

# PostgreSQL local database connection
con_local <- dbConnect(RPostgres::Postgres(), 
                       dbname = "traindb", 
                       host = "localhost", 
                       port = "5432", 
                       user = Sys.getenv("TRAIN_LOCALDB_USER"), 
                       password = Sys.getenv("TRAIN_LOCALPWD"))

## Define last data bulk date
last_bulk_train_data <- dbGetQuery(con_local, "SELECT MAX(data_timestamp) FROM train_data;")
last_bulk_train_schedule <- dbGetQuery(con_local, "SELECT MAX(schedule_timestamp) FROM schedule;")
last_bulk_train_tracking <- dbGetQuery(con_local, "SELECT MAX(obtained_at) FROM tracking;")


# Data load ----
#train_data <- read_delim("D:/Data/train_data/train_data_until_041022.csv", delim = ";", escape_double = FALSE, trim_ws = TRUE)
#train_schedule <- read_delim("D:/Data/train_schedule/train_schedule_until_041022.csv", delim = ";", escape_double = FALSE, trim_ws = TRUE)
#train_tracking <- read_delim("D:/Data/train_tracking/train_tracking_until_041022.csv", delim = ";", escape_double = FALSE, trim_ws = TRUE)
#renfe_stations <- read_delim("D:/Data/renfe_stations.csv", delim = ",", escape_double = FALSE, trim_ws = TRUE)

# Data import from cloud database
q_train_data <- paste0("SELECT * FROM train_data WHERE data_timestamp > ", "'", last_bulk_train_data[[1]] + minutes(5), "';")
q_train_schedule <- paste0("SELECT * FROM train_schedules WHERE schedule_timestamp > ", "'", last_bulk_train_schedule[[1]] + minutes(5), "';")
q_train_tracking <- paste0("SELECT * FROM train_tracking WHERE timestamp > ", "'", last_bulk_train_tracking[[1]] + minutes(5), "';")

train_data <- dbGetQuery(con_cloud, q_train_data)
train_schedule <- dbGetQuery(con_cloud, q_train_schedule)
train_tracking <- dbGetQuery(con_cloud, q_train_tracking)

# Data import from Renfe data site (spanish stations dataset): only first time db creation
#renfe_stations <- read_delim("https://data.renfe.com/dataset/ed3d44e5-1d04-41d6-9aa5-396442bf3e07/resource/783e0626-6fa8-4ac7-a880-fa53144654ff/download/listado-estaciones-completo.csv", 
#                             delim = ";", escape_double = FALSE, trim_ws = TRUE)

# Data preprocessing ----

## Train data table ----
train_data_tbl <- train_data %>% 
  filter(
    !is.na(tech_id),
    !is.na(com_id),
    !is.na(data_timestamp)
  ) %>% 
  transmute(
    id_key = as.character(paste0(gsub("-", "", floor_date(data_timestamp, "day")), tech_id, com_id)),
    id_date = as.integer(paste0(gsub("-", "", floor_date(data_timestamp, "day")))),
    tech_id = as.integer(tech_id),
    com_id = as.integer(com_id),
    line = as.character(line),
    actual_origin_code = as.integer(schedule_origin_code),
    actual_destination_code = as.integer(schedule_destination_code),
    actual_origin_departure_time = as.POSIXct(schedule_origin_departure_time),
    actual_destination_arrival_time = as.POSIXct(schedule_destination_arrival_time),
    last_stop_code = as.integer(last_stop_code),
    last_stop_delay = as.integer(last_stop_delay),
    is_pmr = as.logical(is_pmr),
    is_disabled = as.logical(is_disabled),
    is_cancelled = as.logical(is_cancelled),
    data_timestamp = as.POSIXct(data_timestamp)
    ) %>% 
  group_by(across(-data_timestamp)) %>% 
  arrange(desc(data_timestamp)) %>% 
  distinct(across(-data_timestamp), .keep_all = TRUE)

## Schedule table ----
schedule_tbl <- train_schedule %>%
  filter(
    !is.na(tech_id),
    !is.na(com_id),
    !is.na(schedule_timestamp)
  ) %>% 
  transmute(
    train_id = as.character(paste0(gsub("-", "", floor_date(schedule_timestamp, "day")), tech_id, com_id)),
    stop_code = as.integer(schedule_stop_code),
    stop_arrival_time = lubridate::as_datetime(schedule_stop_arrival_time),
    stop_departure_time = lubridate::as_datetime(schedule_stop_departure_time),
    stop_order = NA,
    schedule_timestamp = lubridate::as_datetime(schedule_timestamp)
  ) %>% 
  arrange(desc(schedule_timestamp)) %>% 
  distinct(across(-schedule_timestamp), .keep_all = TRUE)

### Add stop_order column ----
schedule_tbl <- schedule_tbl %>% 
  group_by(train_id) %>% 
  arrange(desc(stop_arrival_time)) %>% 
  mutate(stop_order = as.integer(rank(stop_arrival_time))) %>% 
  ungroup()

## Tracking table ----
tracking_tbl <- train_tracking %>% 
  filter(
    !is.na(tech_id),
    !is.na(com_id),
    !is.na(timestamp)
  ) %>% 
  transmute(
    train_id = as.character(paste0(gsub("-", "", floor_date(timestamp, "day")), tech_id, com_id)),
    next_stop_code = as.integer(stop_code),
    current_delay = as.integer(stop_delay),
    obtained_at = lubridate::as_datetime(timestamp),
    )

## Stations table ----
stations_tbl <- train_schedule %>% 
  filter(
    !is.na(schedule_stop_code),
    !is.na(schedule_stop_name)
  ) %>% 
  transmute(
    station_id = as.integer(schedule_stop_code),
    station_name = as.character(schedule_stop_name)
  ) %>% 
  distinct()

stations_tbl <- renfe_stations %>% 
  transmute(
    station_id = as.integer(`CÓDIGO`),
    station_name = as.character(DESCRIPCION)
  ) %>% 
  rbind(stations_tbl) %>% 
  distinct(station_id, .keep_all = TRUE)

## Date table ----
dates_range <- train_data %>% 
  select(data_timestamp) %>% 
  filter(!is.na(data_timestamp))

min_date = as.Date(min(floor_date(dates_range$data_timestamp, "day")))
max_date = as.Date(max(floor_date(dates_range$data_timestamp, "day")))

date_tbl <- tibble(
  date_key = as.character(gsub("-", "", seq(min_date, max_date, by = "days"))),
  calendar_date = as.Date(seq(min_date, max_date, by = "days")),
  year = as.integer(lubridate::year(calendar_date)),
  month = as.character(format(calendar_date, "%B")),
  month_num = as.integer(lubridate::month(calendar_date)),
  week = as.integer(lubridate::isoweek(calendar_date)),
  weekday = as.character(weekdays(calendar_date)),
  weekday_num = as.integer(lubridate::wday(calendar_date, week_start = 1))
  ) 

# Filter registries ----
schedule_not_in_fct_table <- setdiff(schedule_tbl$train_id, train_data_tbl$id_key)
tracking_not_in_fct_table <- setdiff(tracking_tbl$train_id, train_data_tbl$id_key)

schedule_tbl <- subset(schedule_tbl,!train_id %in% schedule_not_in_fct_table)
tracking_tbl <- subset(tracking_tbl,!train_id %in% tracking_not_in_fct_table)

# Load data ----

dbWriteTable(con_local, name = "date", value = date_tbl, append = TRUE, row.names = FALSE)
#dbWriteTable(con_local, name = "stations", value = stations_tbl, append = TRUE, row.names = FALSE)
dbWriteTable(con_local, name = "train_data", value = train_data_tbl, append = TRUE, row.names = FALSE)
dbWriteTable(con_local, name = "schedule", value = schedule_tbl, append = TRUE, row.names = FALSE)
dbWriteTable(con_local, name = "tracking", value = tracking_tbl, append = TRUE, row.names = FALSE)

dbDisconnect(con_cloud)
dbDisconnect(con_local)

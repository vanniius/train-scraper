# Libraries
library(tidyverse)
library(lubridate)
library(RPostgres)

# Query data from database
con <- dbConnect(RPostgres::Postgres(), dbname = Sys.getenv("TRAIN_DBNAME"), host = Sys.getenv("TRAIN_HOST"), 
                 port = Sys.getenv("TRAIN_PORT"), user = Sys.getenv("TRAIN_USER"), password = Sys.getenv("TRAIN_PWD"))

train_tracking <- dbGetQuery(con, "SELECT * FROM train_tracking 
                                   WHERE timestamp >= '2022-08-16 00:00:00';")

train_schedule <- dbGetQuery(con, "SELECT * FROM train_schedules 
                                   WHERE schedule_timestamp >= '2022-08-16 00:00:00';")

train_data <- dbGetQuery(con, "SELECT * FROM train_data
                               WHERE data_timestamp >= '2022-08-16 00:00:00';")

str(train_tracking)
str(train_schedule)
str(train_data)

# Data wrangling
train_data %>% 
  rename(actual_origin_departure_time = schedule_origin_departure_time,
         actual_destination_arrival_time = schedule_destination_arrival_time) %>% 
  mutate(service_type = case_when(line %in% c("R1", "R2", "R3", "R4", "R7", "R8", "RT2", "RG1") ~ "Rodalia",
                                  TRUE ~ "Regional"),
         is_cancelled = case_when(is_disabled == FALSE & is_cancelled == FALSE ~ FALSE,
                                  TRUE ~ TRUE),
         is_delay = case_when(last_stop_delay > 3 & service_type == "Rodalia" ~ TRUE,
                              last_stop_delay > 5 & service_type == "Regional" ~ TRUE,
                              TRUE ~ FALSE),
         data_day = floor_date(data_timestamp, "day")) %>% 
  select(line, tech_id, com_id, actual_origin_departure_time,last_stop_code, actual_destination_arrival_time, last_stop_name, 
         last_stop_delay, is_cancelled, is_pmr, is_composition, is_delay, service_type, data_timestamp, data_day) -> eda_data

train_schedule %>% 
  mutate(schedule_day = floor_date(schedule_timestamp, "day")) -> eda_schedule

# Extract only origin station schedule for every train and day
eda_schedule %>%
  select(!schedule_stop_arrival_time, !schedule_stop_departure_time, !schedule_stop_code, !schedule_stop_name) %>% 
  group_by(tech_id, schedule_day) %>% 
  slice_min(schedule_stop_departure_time) %>% 
  distinct(across(-schedule_timestamp), .keep_all = TRUE) %>% 
  inner_join(eda_data, by = c("line", "tech_id", "com_id", "schedule_day" = "data_day")) -> eda_data






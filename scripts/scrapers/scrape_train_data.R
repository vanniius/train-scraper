### Libraries
require(tidyverse)
require(rvest)
require(httr)
require(xml2)
require(lubridate)
require(RPostgres)

# Basic parameters
url_schedules  <- Sys.getenv("URL_SCHEDULES")

con <- dbConnect(RPostgres::Postgres(),
                 dbname = Sys.getenv("TRAIN_DBNAME"),
                 host = Sys.getenv("TRAIN_HOST"), 
                 port = Sys.getenv("TRAIN_PORT"),
                 user = Sys.getenv("TRAIN_USER"), 
                 password = Sys.getenv("TRAIN_PWD"))

date <- paste0(format(Sys.Date(), tz = "Europe/Andorra"))
time <- paste0(format(Sys.time(), tz = "Europe/Andorra"))

### Get today schedules
query <- paste0("SELECT * 
                FROM train_schedules 
                WHERE schedule_origin_departure_time >= ", "'", date, " 00:00:00", "';")
dbGetQuery(con, query) -> train_schedules

### Get today train data already scraped
query <- paste0("SELECT * 
                FROM train_data 
                WHERE schedule_destination_arrival_time >= ", "'", date, " 00:00:00", "';")
dbGetQuery(con, query) -> train_data
dbDisconnect(con)

### Filter trains to scrape
if(hour(Sys.time()) %in% 0:20) {
  
  train_schedules %>% 
    filter(!tech_id %in% train_data$tech_id,
           ymd_hms(time) > schedule_destination_arrival_time + minutes(150)) -> trains_to_scrape
  
} else {
  
  train_schedules %>% 
    filter(!tech_id %in% train_data$tech_id) -> trains_to_scrape
  
}

### Scrape train data
map_df(c(unique(trains_to_scrape$tech_id)), function(i){ 
  
  try({
    
    Sys.sleep(1)
    page <- possibly(read_xml, otherwise = RETRY("GET", url = sprintf(url_schedules, i), pause_base = 1, pause_cap = 5)) (sprintf(url_schedules, i))
    data.frame(line = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page, "line"))),
               tech_id = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"technicalNumber"))),
               com_id = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"commercialNumber"))),
               schedule_origin_code = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"originStation"))),
               schedule_destination_code = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"destinationStation"))),
               schedule_origin_name = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"originStationName"))),
               schedule_destination_name = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page, "destinationStationName"))),
               schedule_origin_departure_time = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"originDateHour"))),
               schedule_destination_arrival_time = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"destinationDateHour"))),
               last_stop_code = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"nextStation"))),
               last_stop_name = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"nextStationName"))),
               last_stop_delay = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"delay"))),
               is_disabled = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"trainDisabled"))),
               is_cancelled = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"trainCancelled"))),
               is_pmr = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"trainAccessible"))),
               is_composition = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"composition"))),
               data_timestamp = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"timestamp")))
               
    )
    
  })
  
}) -> train_data

train_data %>%
  mutate_all(na_if, "") %>% 
  drop_na(tech_id) %>%
  mutate(last_stop_delay = as.numeric(last_stop_delay),
         is_disabled = as.logical(is_disabled),
         is_cancelled = as.logical(is_cancelled),
         is_pmr = as.logical(is_pmr),
         is_composition = as.logical(is_composition),
         schedule_origin_departure_time = dmy_hms(schedule_origin_departure_time),
         schedule_destination_arrival_time = dmy_hms(schedule_destination_arrival_time),
         data_timestamp = dmy_hms(data_timestamp)) -> train_data

# Those trains with huge delays will be parsed at the end of the day
if(hour(Sys.time()) %in% 0:20) {
  
  train_data %>% 
    filter(last_stop_delay < 120) -> train_data
  
}

### Write data
con <- dbConnect(RPostgres::Postgres(),
                 dbname = Sys.getenv("TRAIN_DBNAME"),
                 host = Sys.getenv("TRAIN_HOST"), 
                 port = Sys.getenv("TRAIN_PORT"),
                 user = Sys.getenv("TRAIN_USER"), 
                 password = Sys.getenv("TRAIN_PWD"))

dbWriteTable(con, name = "train_data", value = train_data, append = TRUE, row.names = FALSE)

dbDisconnect(con)
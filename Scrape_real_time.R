### Libraries
require(tidyverse)
require(rvest)
require(httr)
require(xml2)
require(lubridate)
#require(RMariaDB)

### Basic parameters
#setwd("/Users/ivans/Desktop/DS/R Projects/Rodalies")

#source("Secrets.R")

url_departures <- url_scrape_station
url_schedules  <- url_scrape_schedules

### Stations to track
stations <- c(79312, 79309, 79303, 79300, 79202, 79607, 79605, 79602, 79502, 79412, 79406, 71801, 79103, 79007, 79009, 71707, 
              71706, 71701, 71601, 77309, 77306, 77113, 77105, 77005, 78801, 78604, 78710, 78706, 72301, 72211, 72201, 71401, 
              71600, 71303, 71210, 71500, 65402, 72502, 78408, 78503, 78402, 73101, 73008, 73002, 65411)

### Empty df
scrape_station <-
  data.frame(
    line = NA,
    tech_id = NA,
    com_id = NA,
    stop_code = NA,
    stop_name = NA,
    stop_delay = NA,
    timestamp = NA
  )

train_tracking <-
  data.frame(
    line = NA,
    tech_id = NA,
    com_id = NA,
    stop_code = NA,
    stop_name = NA,
    stop_delay = NA,
    timestamp = NA
  )

schedule_tracking <-
  data.frame(
    line = NA,
    tech_id = NA,
    com_id = NA,
    schedule_origin_code = NA,
    schedule_destination_code = NA,
    schedule_origin_name = NA,
    schedule_destination_name = NA,
    schedule_origin_departure_time = NA,
    schedule_destination_arrival_time = NA,
    schedule_stop_code = NA,
    schedule_stop_name = NA,
    schedule_stop_arrival_time = NA,
    schedule_stop_departure_time = NA
  )

train_schedules <-
  data.frame(
    line = NA,
    tech_id = NA,
    com_id = NA,
    schedule_origin_code = NA,
    schedule_destination_code = NA,
    schedule_origin_name = NA,
    schedule_destination_name = NA,
    schedule_origin_departure_time = NA,
    schedule_destination_arrival_time = NA,
    schedule_stop_code = NA,
    schedule_stop_name = NA,
    schedule_stop_arrival_time = NA,
    schedule_stop_departure_time = NA
  )

### Execution time, in minutes
period <- 15
tm <- Sys.time()

### Real time train scraper

while(difftime(Sys.time(), tm, units = "mins")[[1]] < period) {
  
  print(paste0("Start: ", format(Sys.time(), "%H:%M:%S")))
  
  ### Scraping trains by next hour arrivals and departures by station
  
  for(i in 1:length(stations)) {
    
    page <- possibly(read_xml, otherwise = RETRY("GET", url = sprintf(url_departures, stations[i]), pause_base = 1, pause_cap = 5)) (sprintf(url_departures, stations[i]))
    data.frame(line = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page, "line"))),
               tech_id = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"technicalNumber"))),
               com_id = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"commercialNumber"))),
               stop_code = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"nextStation"))),
               stop_name = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"nextStationName"))),
               stop_delay = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"delay"))),
               timestamp = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"timestamp")))
               
    ) -> scrape_station
    
    
    scrape_station %>% 
      mutate_all(na_if, "") %>% 
      drop_na(tech_id) -> scrape_station
    
    ### Adding results to the tracking registry
    
    if(length(scrape_station$tech_id > 1)) {
      
      union(scrape_station, train_tracking) ->> train_tracking
      
    }
    
    Sys.sleep(1)
    
  }
  
  ### Scraping entire schedule for new spotted trains
  
  subset(train_tracking, !tech_id %in% train_schedules$tech_id == TRUE) ->> new_train
  
  if (length(new_train$tech_id > 0)) {
    
    map_df(c(unique(new_train$tech_id)), function(i){ 
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
                 schedule_stop_code = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"stationCode"))),
                 schedule_stop_name = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"stationName"))),
                 schedule_stop_arrival_time = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"arrivalDateHour"))),
                 schedule_stop_departure_time = paste0("", possibly(xml_text, otherwise = NA) (html_elements(page,"departureDateHour")))
                 
      )
      
      
    }) ->> schedule_tracking
    
    schedule_tracking %>% 
      mutate_all(na_if, "") %>% 
      drop_na(tech_id) -> schedule_tracking
    
    # Addign new retrieved schedules to the registry
    
    if(length(schedule_tracking$tech_id > 1)) {
      
      union(schedule_tracking, train_schedules) ->> train_schedules
      
    }
    
  }
  
  print(paste0("End: ", format(Sys.time(), "%H:%M:%S")))
  
  Sys.sleep(240)
  
}

### Consolidate data
train_tracking %>% 
  select(tech_id, stop_code, stop_name, stop_delay, timestamp) %>% 
  group_by(tech_id, stop_code) %>% 
  slice_max(timestamp) %>% 
  distinct() %>% 
  ungroup(tech_id, stop_code) %>% 
  full_join(train_schedules, by = c("tech_id", "stop_name" = "schedule_stop_name")) %>% 
  select(line, tech_id, stop_name, stop_delay, schedule_stop_departure_time, timestamp) -> train_data

write_csv(train_data, "data/train_consolidated_data.csv", append = TRUE)
write_csv(train_tracking, "data/train_tracking_data.csv", append = TRUE)
write_csv(train_schedules, "data/train_schedules_data.csv", append = TRUE)


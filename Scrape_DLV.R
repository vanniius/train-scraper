# Libraries
require(tidyverse)
require(rvest)
require(httr)
require(xml2)
require(RMariaDB)

# Basic parameters
setwd("/Users/ivans/Desktop/DS/R Projects/Rodalies")

source("Secrets.R")
url_base <- url_scrape_train

# Obtain today's schedule
HORARIS_DIA <- read_delim("Data/HORARIS_DIA.csv", delim = ";", escape_double = FALSE, trim_ws = TRUE)

# Scraper
map_df(c(HORARIS_DIA$IDT), function(i){
  Sys.sleep(runif(1, 0, 1))
  if(i %% 100 == 0) {Sys.sleep(20)}
  page <- possibly(read_xml, otherwise = RETRY("GET", url = sprintf(url_base,i), pause_base = 10, pause_cap = 30)) (sprintf(url_base,i))
  data.frame(LINIA = possibly(xml_text, otherwise = NA) (html_element(page, "line")),
             IDT = possibly(xml_text, otherwise = NA) (html_element(page,"commercialNumber")),
             IDC = possibly(xml_text, otherwise = NA) (html_element(page,"technicalNumber")),
             ORIGEN_REAL = possibly(xml_text, otherwise = NA) (html_element(page,"originStationName")),
             DESTINACIO_REAL = possibly(xml_text, otherwise = NA) (html_element(page, "destinationStationName")),
             SORTIDA_REAL = possibly(xml_text, otherwise = NA) (html_element(page,"originDateHour")),
             ARRIBADA_REAL = possibly (xml_text, otherwise = NA) (html_element(page, "destinationDateHour")),
             RETARD = possibly(xml_text, otherwise = NA) (html_element(page,"delay")),
             IS_PMR = possibly(xml_text, otherwise = NA) (html_element(page,"trainAccessible")),
             IS_COMPOSICIO = possibly(xml_text, otherwise = NA) (html_element(page,"composition")),
             IS_SUPRIMIT = possibly(xml_text, otherwise = NA) (html_element(page,"trainDisabled")),
             IS_CANCELAT = possibly(xml_text, otherwise = NA) (html_element(page,"trainCancelled")),
             OBSERVACIONS = possibly(xml_text, otherwise = NA) (html_element(page,"trainObservations")),
             TIMESTAMP = possibly(xml_text, otherwise = NA) (html_element(page,"timestamp"))
  )
}) -> CIRCULACIONS_DIA

# Some transformations
CIRCULACIONS_DIA %>% 
  drop_na(IDT, IDC) %>% 
  mutate(KEY = paste0(IDT, "-", gsub("-", "", Sys.Date())),
         IS_SUPRIMIT = case_when(IS_SUPRIMIT == "true" | IS_CANCELAT == "true" ~ "TRUE",
                                 TRUE ~ "FALSE"),
         IS_PMR = toupper(IS_PMR),
         IS_COMPOSICIO = toupper(IS_COMPOSICIO)) %>%
  select(-IS_CANCELAT) -> CIRCULACIONS_DIA

# Connect and write results into the database
con <- dbConnect(MariaDB(), user = user_db, password = pwd_db, db = "roddb")
dbExecute(con, "SET GLOBAL local_infile = 1;")
dbWriteTable(con, name = "circulacions", value = CIRCULACIONS_DIA, append = TRUE, row.names = FALSE)
dbDisconnect(con)

# Num. of trains parsed
cat("Done! This time", length(CIRCULACIONS_DIA$IDT), "trains were parsed.")

            
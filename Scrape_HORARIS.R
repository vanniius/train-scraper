# Libraries
require(tidyverse)
require(lubridate)
require(rvest)
require(httr)
require(xml2)
require(RMariaDB)

# Basic parameters
source("Secrets.R")

setwd(wd)
url_base <- url_scrape_train

# Obtain today's schedule
HORARIS_DIA <- read_delim("Data/HORARIS_DIA.csv", delim = ";", escape_double = FALSE, trim_ws = TRUE)

# Evaluate which trains to parse according to day of week
if (wday(Sys.Date()) %in% 2:6) {
  
  trains <- c(15000:15100, 15150:15200, 15500:15510, 15600:15640, 15800:15850, 15900:15920, 18050:18100, 25000:25200, 25300:25900,
              28000:28200, 28250:28300, 28350:28700, 28800:28950, 30850:30900, 33040:33050, 34600:34700, 33500:33530, 33850:33860, 
              77000:79000)
} else {
  
  trains <- c(15000:15160, 15230:15250, 15600:15630, 15800:15850, 15900:15920, 18050:18100, 18250:18270, 25200:25260, 25400:25570, 
              25870:25970, 28200:28500, 28550:28760, 28900:29000, 32820:32900, 33510:33520, 77000:79000)
}

# Scraper
map_df(c(trains), function(i){
  Sys.sleep(runif(1, 0, 1))
  if(i %% 100 == 0) {Sys.sleep(20)}
  page <- possibly(read_xml, otherwise = RETRY("GET", url = sprintf(url_base,i), pause_base = 10, pause_cap = 30)) (sprintf(url_base,i))
  data.frame(LINIA = possibly(xml_text, otherwise = NA) (html_element(page, "line")),
             IDC = possibly(xml_text, otherwise = NA) (html_element(page,"commercialNumber")),
             IDT = possibly(xml_text, otherwise = NA) (html_element(page,"technicalNumber")),
             ORIGEN_HORARIS = possibly(xml_text, otherwise = NA) (html_element(page,"originStationName")),
             DESTINACIO_HORARIS = possibly(xml_text, otherwise = NA) (html_element(page, "destinationStationName")),
             SORTIDA_HORARIS = possibly(xml_text, otherwise = NA) (html_element(page,"originDateHour")),
             ARRIBADA_HORARIS = possibly(xml_text, otherwise = NA) (html_element(page,"destinationDateHour")),
             TIMESTAMP = possibly(xml_text, otherwise = NA) (html_element(page,"timestamp"))
  )
}) -> HORARIS_DIA

# Some transformations
HORARIS_DIA %>% 
  drop_na(IDT, IDC) %>% 
  mutate(KEY = paste0(IDT, "-", gsub("-", "", Sys.Date()))) -> HORARIS_DIA

# Write into csv (temporarily)
write_excel_csv2(HORARIS_DIA, file = "Data/HORARIS_DIA.csv")

# Connect and write results into the database
con <- dbConnect(MariaDB(), user = user_db, password = pwd_db, db = "roddb")
dbExecute(con, "SET GLOBAL local_infile = 1;")
dbWriteTable(con, name = "horaris", value = HORARIS_DIA, append = TRUE, row.names = FALSE)
dbDisconnect(con)

# Num. of trains parsed
cat("Done! This time", length(HORARIS_DIA$IDT), "trains were parsed.")
  

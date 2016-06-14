library(dplyr)
library(stringr)

# download the cal-access file
if (!dir.exists("data")) dir.create("data", recursive = TRUE)
download.file("http://www.sos.ca.gov/campaignfinance/dbwebexport.zip",
              dest = "data/cal_access.zip")

# and unzip -- starting with just RCPT_CD.TSV, which has transactions
unzip("data/cal_access.zip", files = "CalAccess/DATA/RCPT_CD.TSV", 
      exdir = "data", junkpaths = TRUE)

# build documentation folder (will create "docs" directory if needed):
# will overwrite existing docs
if (!dir.exists("docs")) dir.create("docs", recursive = TRUE)
docpaths <- unzip("data/cal_access.zip", list = TRUE) %>%
    filter(!str_detect(Name, "/DATA/")) %>% "$"(Name)
unzip("data/cal_access.zip", files = docpaths, overwrite = TRUE,
      exdir = "docs")

# this script will only work on windows
# it does the following:
# - check the transaction file for malformed entries and remove them
#   (outputting both clean file and the errorlog for review)
# - drop any existing cal_access schema in your postgres database
# - create the cal_access schema and proper ca_rcpt table
# - populate the cal_access.ca_rcpt table with the cleaned up transactions
shell("load_tables.bat")

# to use dplyr, set up the src using src_postgres
pg <- src_postgres(dbname = "postgres", host = "localhost",
                   port = 5432, user = "postgres", password = "postgres",
                   options="-c search_path=cal_access")

# now ca is our tbl
ca <- tbl(pg, "ca_rcpt")

# and now it's easy to grab individual transactions since 1/1/2014
# and load them into memory with dplyr::collect()
cal <- ca %>%
    filter(entity_cd == "IND", 
           rcpt_date >= '2014-01-01') %>%
    group_by(filing_id) %>%
    mutate(last_amend = max(amend_id)) %>%
    ungroup %>%
    filter(amend_id == last_amend) %>%
    collect(n = Inf)
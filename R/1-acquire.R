library(tidyverse)

# download the cal-access file
if (!dir.exists("data")) dir.create("data", recursive = TRUE)
download.file("http://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
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

ca_cols <- cols(
    .default = col_character(),
    FILING_ID = col_integer(),
    AMEND_ID = col_integer(),
    LINE_ITEM = col_integer(),
    RCPT_DATE = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
    DATE_THRU = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
    AMOUNT = col_double(),
    CUM_YTD = col_double(),
    CUM_OTH = col_double()
)

## how far back to pull records
## i like to go back a few years at least, both to catch older transactions
## for newly added entities, and to give enough data to estimate match-weights
mindate <- lubridate::ymd('2015-01-01')

ca_callback <-   function(chunk, pos) {
    filter(chunk, 
           RCPT_DATE >= mindate, 
           RCPT_DATE <= lubridate::today(),
           ENTITY_CD == "IND")
}

# read in the data a chunk at a time, only keep the records that are 
# within the 
cal <- readr::read_tsv_chunked(
    "data/RCPT_CD.TSV", 
    callback = DataFrameCallback$new(ca_callback),
    col_types = ca_cols, col_names = TRUE,
    chunk_size = 1000000)

cal <- set_names(cal, str_to_lower)

# quick check: is the data actually getting updated as expected?
# do recent months have the expected number of 
# records relative to other months, with the assumption that it takes  
# several months for most transactions to get filed and represented in the data
# note that the values will increase leading up to an election/primary, 
# and decrease immediately after
cal %>% 
    select(entity_cd, rcpt_date, amount) %>% 
    mutate(yr = lubridate::year(rcpt_date), 
           mo = lubridate::month(rcpt_date)) %>% 
    group_by(yr, mo) %>% 
    summarise(n = n(), dollars = sum(amount, na.rm = TRUE)) %>% 
    arrange(desc(yr), desc(mo))

# only keep the most recently amended version of each transaction
cal <- cal %>% 
    group_by(filing_id) %>%
    mutate(last_amend = max(amend_id)) %>%
    ungroup %>%
    filter(amend_id == last_amend)
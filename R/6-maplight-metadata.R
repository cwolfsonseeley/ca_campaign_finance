# after running the match stuff, we want to update metadata tables,
# unfortunately, the best metadata is not available through cal-access,
# but through maplight. 

# let's start clean
rm(list = ls())

library(tidyverse)
library(rvest)
library(ratelimitr)

# helper functions to download files without hitting rate limits
downloader <- function(filename, url) {
    if (!dir.exists("maplight")) dir.create("maplight")
    destpath = file.path("maplight", filename)
    download.file(url, destfile = destpath, quiet = TRUE)
}

dl <- limit_rate(downloader, rate(n = 1, period = 1))

# find locations of data from maplight, and download
ml <- xml2::read_html("https://maplight.org/data_guide/california-money-and-politics-bulk-data-set/")

items <- ml %>%
    html_nodes("#content li a")

to_dl <- data_frame(
    item = items %>% html_text,
    link = items %>% html_attr("href")
)

to_dl <- to_dl %>% 
    mutate(filename = str_replace_all(item, "[^0-9-]", "") %>% str_trim) %>%
    mutate(filename = paste0(filename, str_extract(link, "[^//]+\\.zip")))

# probably  not always necessary to update all metadata, since older stuff 
# shouldn't be changing. so filter for just the rows you want to update

# start by removing any old data from maplight
if (dir.exists("maplight")) unlink("maplight", recursive = TRUE)

to_dl <- to_dl %>% 
    filter(item == "2017-18 cycle") %>% 
    mutate(downloaded = map2(filename, link, dl))

list.files("maplight", full.names = TRUE) %>%
    keep(str_detect(., "\\.zip$")) %>% 
    map(unzip, exdir = "maplight/unzipped")


cands <- list.files("maplight/unzipped/", full.names = TRUE) %>%
    keep(str_detect(., "cand_")) %>%
    map(read_delim, delim = ",", escape_backslash = TRUE, escape_double = FALSE)

candidates <- cands %>% 
    map(. %>% transmute(transaction_id = TransactionID, 
                        committee = RecipientCommitteeNameNormalized, 
                        candidate = RecipientCandidateNameNormalized,
                        office = RecipientCandidateOffice,
                        district = as.character(RecipientCandidateDistrict))) %>%
    bind_rows

candidates <- candidates %>% 
    mutate(filing_id = str_extract(transaction_id, "^[^\\s]+") %>% str_trim %>% as.integer,
           district = as.integer(district)) %>% 
    select(filing_id, committee, candidate, office, district) %>% 
    distinct %>% 
    #group_by(filing_id) %>% filter(n() > 1) %>% ungroup %>% arrange(filing_id)
    group_by(filing_id) %>%
    top_n(1, wt = committee) %>%
    ungroup

cdw_devel <- getcdw::connect("URELUAT_DEVEL")
getcdw::get_cdw("delete from rdata.ca_campaign_candidate_stage", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)

Sys.setenv(TZ = "DB_TZ")
Sys.setenv(ORA_SDTZ = "DB_TZ")

ROracle::dbWriteTable(
    cdw_devel, "CA_CAMPAIGN_CANDIDATE_STAGE", 
    candidates,
    schema = "RDATA",
    overwrite = FALSE, append = TRUE
)
ROracle::dbCommit(cdw_devel)
getcdw::get_cdw(
    "
    insert into rdata.ca_campaign_candidate
    select 
    stg.*, 
    'CA' || ora_hash(stg.candidate, 4294967295, 20180313) as candidate_id 
    from rdata.ca_campaign_candidate_stage stg
    left join rdata.ca_campaign_candidate ca on stg.filing_id = ca.filing_id
    where ca.filing_id is null
    ", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)


### now do same but for propsitions instead of candidates

inits <- list.files("maplight/unzipped/", full.names = TRUE) %>%
    keep(str_detect(., "other")) %>%
    map(read_delim, delim = ",", escape_backslash = TRUE, escape_double = FALSE)

initiatives <- inits %>% 
    map(. %>% transmute(election_cycle = ElectionCycle,
                        transaction_id = TransactionID, 
                        committee = RecipientCommitteeNameNormalized, 
                        initiative = Target,
                        position = Position))

initiatives <- initiatives %>% bind_rows

# note that a single filing could go to multiple initatives,
# but there shouldn't be duplicates at the filing+initative level
initiatives <- initiatives %>% 
    mutate(filing_id = str_extract(transaction_id, "^[^\\s]+") %>% str_trim %>% as.integer) %>% 
    select(filing_id, election_cycle, committee, initiative, position) %>% 
    distinct %>% 
    mutate(proposition = str_match(
        initiative, regex("prop(osition)? ([^\\-]+)", 
                          ignore_case = TRUE)) %>% "["(,3) %>% str_trim) %>%
    filter(!is.na(proposition)) %>%
    mutate(proposition_desc = str_match(initiative, "\\s?-\\s?(.+)$") %>% "["(, 2) %>% str_trim) %>%
    mutate(proposition_id = paste0("BL", election_cycle, 
                                   str_pad(proposition, width = 4, 
                                           side = "left", pad = "0"))) %>%
    select(filing_id, election_cycle, proposition, proposition_id, 
           proposition_desc, position, committee) %>%
    distinct

# check: filing+proposition, should be no duplicates
initiatives %>%
    group_by(filing_id, proposition) %>% filter(n() > 1)

# next: propositions -> staging tables, then into main ca_campaign_proposition ...

cdw_devel <- getcdw::connect("URELUAT_DEVEL")
getcdw::get_cdw("delete from rdata.ca_campaign_proposition_stage", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)

Sys.setenv(TZ = "DB_TZ")
Sys.setenv(ORA_SDTZ = "DB_TZ")

ROracle::dbWriteTable(
    cdw_devel, "CA_CAMPAIGN_PROPOSITION_STAGE", 
    initiatives,
    schema = "RDATA",
    overwrite = FALSE, append = TRUE
)
ROracle::dbCommit(cdw_devel)
getcdw::get_cdw(
    "
    insert into rdata.ca_campaign_proposition
    select 
    stg.*
    from rdata.ca_campaign_proposition_stage stg
    left join rdata.ca_campaign_proposition ca 
    on stg.filing_id = ca.filing_id
    and stg.proposition = ca.proposition
    where ca.filing_id is null
    ", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)


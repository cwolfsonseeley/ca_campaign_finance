library(preprocessr)
library(magrittr)
####

# pre-process the table to pull out names and other identifying info
# convert everything to lower case (except for state code) for easier compares
cal %<>% 
    mutate(last = str_extract(ctrib_naml, "^[^,]+")) %>%
    mutate(last = str_replace(last, "Jr\\.?$", ""),
           last = str_replace(last, "I+$", ""),
           last = str_trim(tolower(last))) %>%
    mutate(first = str_extract(ctrib_namf, "^[^\\s]+"),
           first = str_trim(tolower(first))) %>%
    mutate(middle_initial = str_extract(ctrib_namf, "\\s+."),
           middle_initial = str_trim(tolower(middle_initial))) %>%
    mutate(zip = tidy_zip(ctrib_zip4),
           employer = str_trim(tolower(ctrib_emp)), 
           occupation = str_trim(tolower(ctrib_occ)),
           city = str_trim(tolower(ctrib_city)),
           state = str_trim(toupper(ctrib_st))) %>%
    mutate(first = str_replace_all(first, "[^a-z]", ""),
           last  = str_replace_all(last,  "[^a-z]", "")) %>%
    select(first, middle_initial, last, 
           employer, occupation, city, state, zip,
           filing_id, amend_id, line_item, form_type, tran_id, tran_type,
           rcpt_date, amount, ctrib_dscr)

# drop any records without a first/last name, won't be able to match anyways
cal %<>% filter(!is.na(first), !is.na(last))

# and create a listing of "unique" individuals for proper matching
# note this is approximate, surely distinct ppl with the same first/last/zip
# exist. 
unique_individuals <- cal %>%
    select(first, last, zip) %>%
    distinct

## an ad-hoc ID field
unique_individuals %<>%
    mutate(ca_id = seq(nrow(.)))

# add id field to larger dataset
cal %<>%
    inner_join(unique_individuals, by = c("first", "last", "zip"))

# frequency tables for frequency-based matching 
# (see 5-match.R for weight calculations)
ca_frequency_first <- unique_individuals %>%
    group_by(first) %>%
    summarise(ca_first = n()) %>%
    mutate(n_ca_first = sum(ca_first))

ca_frequency_last <- unique_individuals %>%
    group_by(last) %>%
    summarise(ca_last = n()) %>%
    mutate(n_ca_last = sum(ca_last))
library(getcdw)
source("R/sql_queries.R")

# pulling cads data and processing it to match to cal-access data
cads_names <- get_cdw(cads_names_query)
cads_address <- get_cdw(cads_address_query)
cads_employment <- get_cdw(cads_employment_query)

# pre-process names to be able to match to cal-access 
# (see 2-process.R for the cal-access processing step)
cads_names %<>%
    mutate_each(funs(tolower), -entity_id) %>%
    mutate(first_name = str_replace_all(first_name, "[^a-z]", ""),
           last_name  = str_replace_all(last_name, "[^a-z]", "")) %>%
    mutate(middle_initial = str_trim(str_sub(middle_name, 1, 1)))

# match should count if the only difference is upper/lower case, 
# so to make it easy, converting everything to lower
cads_address %<>%
    mutate(city = tolower(city))

cads_employment %<>%
    mutate(job_title = tolower(job_title),
           employer  = tolower(employer))

# and building the corresponding frequency tables
# (see 5-match.R for frequency-based weight calculations)
cads_frequency_first <- cads_names %>%
    select(entity_id, first_name) %>%
    distinct %>%
    group_by(first_name) %>%
    summarise(cads_first = n_distinct(entity_id)) %>%
    mutate(n_cads_first = sum(cads_first))

cads_frequency_last <- cads_names %>%
    select(entity_id, last_name) %>%
    distinct %>%
    group_by(last_name) %>%
    summarise(cads_last = n_distinct(entity_id)) %>%
    mutate(n_cads_last = sum(cads_last))
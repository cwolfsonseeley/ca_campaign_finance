# create candidates by: 
# any exact match on first-soundex & last name
library(tidyr)
library(stringdist)

# add soundex fields to do soundex-match
unique_individuals %<>% mutate(first_soundex = soundex(first))
cads_names %<>% mutate(first_name_soundex = soundex(first_name))

# candidates (potential matches) are records where last name matches exactly, 
# and first name is a soundex match
name_candidates <- unique_individuals %>%
    inner_join(cads_names, by = c("first_soundex" = "first_name_soundex",
                                  "last"  = "last_name")) %>%
    select(ca_id, entity_id) %>%
    distinct

# now take the candidates and bring in all of the necessary comparison fields
cal %>% 
    select(ca_id, 
           ca_first = first, 
           ca_mi = middle_initial, 
           ca_last = last, 
           ca_city = city, 
           ca_zip = zip, 
           ca_employer = employer, 
           ca_occupation = occupation) %>%
    distinct %>%
    inner_join(name_candidates, by = "ca_id") %>%
    inner_join(cads_names, by = "entity_id") %>%
    left_join(cads_address, by = "entity_id") %>%
    left_join(cads_employment, by = "entity_id") -> candidate_matrix

candidate_matrix %<>%
    rename(cads_first = first_name,
           cads_mi = middle_initial,
           cads_last = last_name,
           cads_city = city,
           cads_zip = zipcode,
           cads_zip5 = zipcode5,
           cads_employer = employer,
           cads_occupation = job_title)

# define 1/0 match status on each field. for most fields, this is defined as 
# exact match or not, but for employer and occupation, we look for approximate
# matches, using cosine distance and 3 characters at a time. 
candidate_matrix %>%
    transmute(ca_id, entity_id,
              first = ca_first == cads_first,
              mi = ca_mi == cads_mi,
              last = ca_last == cads_last,
              geo = ca_city == cads_city | ca_zip == cads_zip5,
              occupation = ca_occupation != "" & cads_occupation != "" & 
                  stringdist(ca_occupation, cads_occupation, "cosine", q = 3) < .5,
              employer = ca_occupation != "" & cads_occupation != "" & 
                  stringdist(ca_employer, cads_employer, "cosine", q = 3) < .5) %>%
    replace_na(list(first = FALSE, mi = FALSE, last = FALSE, geo = FALSE, 
                    occupation = FALSE, employer = FALSE)) %>%
    group_by(ca_id, entity_id) %>%
    summarise_all(funs(max)) %>% ungroup -> gamma_matrix

# tmp <- gamma_matrix %>% 
#     filter(occupation > 0 | employer > 0) %>%  
#     inner_join(candidate_matrix, by = c("ca_id", "entity_id")) %>%
#     select(ca_id, entity_id, ca_employer, ca_occupation,
#            cads_employer, cads_occupation) %>%
#     mutate(cads_employer = str_trim(cads_employer),
#            cads_occupation = str_trim(cads_occupation))
# 
# tmp <- tmp %>%
#     mutate(occupation = ca_occupation != "" & cads_occupation != "" & 
#                stringdist(ca_occupation, cads_occupation, "cosine", q = 3) < .5,
#            employer = ca_occupation != "" & cads_occupation != "" & 
#                stringdist(ca_employer, cads_employer, "cosine", q = 3) < .5) %>%
#     replace_na(list(occupation = FALSE, employer = FALSE)) %>%
#     group_by(ca_id, entity_id) %>%
#     summarise(occupation = max(occupation), employer = max(employer)) %>%
#     ungroup
# 
# tmp <- tmp %>% rename(t.occupation = occupation, t.employer = employer)
# 
# gamma_matrix <- gamma_matrix %>% left_join(tmp, by = c("ca_id", "entity_id")) %>%
#     mutate(occupation = ifelse(is.na(t.occupation), occupation, t.occupation),
#            employer = ifelse(is.na(t.employer), employer, t.employer)) %>%
#     select(ca_id:employer)

#####
# calculate the agree/disagree weights for the non-name fields
# (name matches have weights that are dependent on the relative frequency
#  of the name)
source("R/fs-model-functions.R")
preds = names(gamma_matrix)[6:8]
guess_u <- rep(.1, length(preds))
names(guess_u) <- preds
guess_p <- .1
guess_m <- rep(.9, length(preds))
names(guess_m) <- preds

df <- gamma_matrix %>%
    select_(.dots=preds) %>%
    group_by_(.dots=preds) %>%
    summarise(count=n()) %>%
    ungroup

ans <- fs_weights(df, preds, "count", guess_p, guess_m, guess_u)

m <- ans$m
u <- ans$u
p <- ans$p
m[m>.999] <- .999
u <- pmin(u, random_agreement(df, preds, "count"))
u[u<.005] <- .005
agree_weight <- log2(m/u)
disagree_weight <- log2((1-m)/(1-u))

if (!dir.exists("matched/matchweights")) dir.create("matched/matchweights")
library(lubridate)
saveRDS(
    list(agree_weight = agree_weight,
         disagree_weight = disagree_weight),
    file = paste0("matched/matchweights/weights-", year(today()), 
                  stringr::str_pad(month(today()), width = 2, pad = "0"),
                  stringr::str_pad(day(today()), width = 2, pad = "0"), ".rds")
)
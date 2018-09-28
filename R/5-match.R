## use frequency tables to create conditional probabilities
## these are for frequency based weight adjustments on first/last name
first_wt <- ca_frequency_first %>%
    inner_join(cads_frequency_first, by = c("first" = "first_name")) %>%
    mutate(n_ca_first = as.numeric(n_ca_first), n_cads_first = as.numeric(n_cads_first)) %>%
    mutate(h = ifelse(ca_first > 1 | cads_first > 1,
                      pmin(ca_first, cads_first),
                      .6)) %>%
    mutate(nab = sum(h),
           m_first = .98 * h / sum(h),
           u_first = .98 * (ca_first * cads_first - h) / (n_ca_first * n_cads_first - sum(h))) %>%
    mutate(weight = log2(m_first / u_first)) %>%
    select(first_name = first, first_weight = weight)


last_wt <- ca_frequency_last %>%
    inner_join(cads_frequency_last, by = c("last" = "last_name")) %>%
    replace_na(list(cads_last = 0, n_cads_last = 0,
                    ca_last = 0, n_ca_last = 0)) %>%
    mutate(h = ifelse(ca_last > 1 | cads_last > 1,
                      pmin(ca_last, cads_last),
                      .6)) %>%
    mutate(m_last = h / sum(h),
           u_last = .98 * (ca_last * cads_last - h) / (n_ca_last * n_cads_last - sum(h))) %>%
    mutate(wt = log2(m_last / u_last)) %>%
    select(last_name = last, 
           last_weight = wt)

saveRDS(
    list(first_wt = first_wt,
         last_wt = last_wt),
    file = paste0("matched/matchweights/nameweights-", year(today()), 
                  stringr::str_pad(month(today()), width = 2, pad = "0"),
                  stringr::str_pad(day(today()), width = 2, pad = "0"), ".rds")
)

# apply agree/disagree weights, as well as frequency-based name match weights
candidate_matrix %>%
    transmute(ca_id, entity_id, ca_first, cads_first, ca_last, cads_last,
              wt_geo = ifelse(ca_city == cads_city | ca_zip == cads_zip5, agree_weight["geo"], disagree_weight["geo"]),
              wt_occ = ifelse(stringdist(ca_occupation, cads_occupation, "cosine", q = 3) < .5,
                              agree_weight[["occupation"]], disagree_weight[["occupation"]]),
              wt_emp = ifelse(stringdist(ca_employer, cads_employer, "cosine", q = 3) < .5,
                              agree_weight[["employer"]], disagree_weight[["employer"]])) %>%
    inner_join(first_wt, by = c("ca_first" = "first_name")) %>%
    inner_join(last_wt, by = c("ca_last" = "last_name")) %>%
    mutate(wt_first = ifelse(
        stringdist(ca_first, cads_first, method = "jw", p = .1) < .2, first_weight, -5),
        wt_last  = ifelse(ca_last  == cads_last, last_weight, -5)) -> matchscore

matchscore %<>% 
    replace_na(list(wt_geo = 0, wt_occ = 0, wt_emp = 0, 
                    wt_first = 0, wt_last = 0))

# and just take pairs with the highest match score
matchscore %>%
    group_by(ca_id, entity_id) %>%
    summarise(geo = max(wt_geo), occ = max(wt_occ), emp = max(wt_emp), 
              first = max(wt_first), last = max(wt_last)) %>%
    ungroup %>%
    # left_join(tmp, by = c("ca_id", "entity_id")) %>% 
    # mutate(occ = ifelse(!is.na(t.occupation) & t.occupation < 1,
    #                     disagree_weight[["occupation"]], occ),
    #        emp = ifelse(!is.na(t.employer) & t.employer < 1,
    #                     disagree_weight[["employer"]], emp)) %>%
    # select(ca_id:last) %>%
    mutate(score = geo + occ + emp + first + last) %>%
    group_by(ca_id) %>%
    mutate(maxscore = max(score)) %>%
    ungroup %>%
    filter(score == maxscore) -> matchdict

# the amounts here are arbitrary, should experiment from time to time
# to measure error rates using different cutoffs.
matchdict %>% 
    filter(score >= 28 | (first > 0 & last > 0 & geo > 0 & score > 25)) %>% 
    select(ca_id, entity_id) %>%
    distinct -> idmap

# a handful of ambiguous matches can be resolved by using middle initial and/or
# first name similarity (if any blocking step doesn't include first name) as a 
# tie-breaker
idmap <- cal %>% 
    mutate(ca_mi = middle_initial) %>%
    inner_join(idmap, by = "ca_id") %>%
    inner_join(cads_names, by = "entity_id") %>%
    mutate(mi_score = ifelse(is.na(ca_mi) | is.na(middle_initial.y) |
                                 str_length(str_trim(ca_mi)) == 0 |
                                 str_length(str_trim(middle_initial.y)) == 0, 0,
                             ifelse(ca_mi == middle_initial.y, 1, -1))) %>%
    mutate(fname_sim = 1 - stringdist(first, first_name, method = "jw", p = .1)) %>%
    select(ca_id, entity_id, mi_score, fname_sim) %>%
    distinct %>%
    group_by(ca_id) %>%
    mutate(maxsim = max(fname_sim)) %>%
    ungroup %>%
    filter(fname_sim == maxsim) %>%
    group_by(ca_id) %>%
    mutate(maxmi = max(mi_score)) %>%
    ungroup %>% 
    filter(mi_score == maxmi) %>% 
    select(ca_id, entity_id) %>%
    distinct

cal %>% 
    mutate(ca_mi = middle_initial) %>%
    inner_join(idmap, by = "ca_id") %>%
    inner_join(cads_names, by = "entity_id") %>%
    select(ca_id, entity_id, first, ca_mi, 
           last:ctrib_dscr) %>%
    distinct -> all_cads_ca

cdw_devel <- getcdw::connect("URELUAT_DEVEL")
getcdw::get_cdw("delete from rdata.ca_campaign_stage", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)

Sys.setenv(TZ = "DB_TZ")
Sys.setenv(ORA_SDTZ = "DB_TZ")

ROracle::dbWriteTable(
    cdw_devel, "CA_CAMPAIGN_STAGE", 
    all_cads_ca %>% 
        select(entity_id, filing_id, amend_id, line_item, 
               form_type, tran_id, tran_type, rcpt_date, amount),
    schema = "RDATA",
    overwrite = FALSE, append = TRUE
)
ROracle::dbCommit(cdw_devel)


######## DO THIS FIX THIS DO THIS FIX THIS ########################
getcdw::get_cdw("
delete from rdata.ca_campaign ca
where exists (
                select ca.* from rdata.ca_campaign_stage stg
                where ca.entity_id = stg.entity_id
                and ca.filing_id = stg.filing_id
                and ca.tran_id = stg.tran_id
                and ca.amend_id < stg.amend_id
)
", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)
# 
# insert new records
# insert into rdata.ca_campaign
# select stg.* from rdata.ca_campaign_stage stg
# left join rdata.ca_campaign ca 
# on stg.entity_id = ca.entity_id
# and stg.filing_id = ca.filing_id
# and stg.line_item = ca.line_item
# where ca.entity_id is null

getcdw::get_cdw(
"
insert into rdata.ca_campaign
select stg.* 
from 
    rdata.ca_campaign_stage stg
    left join rdata.ca_campaign ca
        on stg.entity_id = ca.entity_id
        and stg.filing_id = ca.filing_id
        and stg.tran_id = ca.tran_id
where ca.entity_id is null        
", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)
# information about each transaction in campaign disclosure cover sheet
library(readr)
unzip("data/cal_access.zip", files = "CalAccess/DATA/CVR_CAMPAIGN_DISCLOSURE_CD.TSV", 
      exdir = "data", junkpaths = TRUE)
cvr <- readr::read_tsv("data/CVR_CAMPAIGN_DISCLOSURE_CD.TSV")
cvr <- cvr[-problems(cvr)$row, ]
names(cvr) <- tolower(names(cvr))

ca_campaign_cvr <- cvr %>% 
    select(filing_id, filer_id, amend_id, filer_naml, filer_namf, 
           cand_naml, cand_namf, bal_name:sup_opp_cd) %>%
    distinct

cdw_devel <- getcdw::connect("URELUAT_DEVEL")
getcdw::get_cdw("delete from rdata.ca_campaign_cvr_stage", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)

Sys.setenv(TZ = "DB_TZ")
Sys.setenv(ORA_SDTZ = "DB_TZ")

ROracle::dbWriteTable(
    cdw_devel, "CA_CAMPAIGN_CVR_STAGE", 
    ca_campaign_cvr,
    schema = "RDATA",
    overwrite = FALSE, append = TRUE
)
ROracle::dbCommit(cdw_devel)
getcdw::get_cdw(
    "
insert into rdata.ca_campaign_cvr
select stg.* 
from 
    rdata.ca_campaign_cvr_stage stg
    left join rdata.ca_campaign_cvr ca
        on stg.filing_id = ca.filing_id
        and stg.amend_id = ca.amend_id
where ca.filing_id is null        
", dsn = "URELUAT_DEVEL")
ROracle::dbCommit(cdw_devel)

all_cads_ca %<>%
    left_join(cvr, by = c("filing_id" = "filing_id", 
                          "amend_id"  = "amend_id")) %>%
    select(ca_id:ctrib_dscr,
           filer_naml, cand_naml, cand_namf, bal_name:sup_opp_cd) %>%
    rename(employer = employer.x, occupation = occupation.x)

# example query -- find most popular ballot initiatives, by # of donors
all_cads_ca %>% 
    filter(!is.na(bal_name)) %>%
    group_by(filer_naml) %>% 
    summarise(total = sum(amount), 
              n = n_distinct(entity_id)) %>%
    ungroup %>%
    arrange(desc(n))

# check that max rcpt_date is relatively recent
all_cads_ca %>% filter(rcpt_date <= Sys.time()) %>% summarise(maxdt = max(rcpt_date))

# create an output file
# name/degrees/capacity not really necessary, as long as we have the ID
names_q <- "
select entity_id, report_name as name, degree_major_year as degrees, capacity_rating_desc as capacity from cdw.d_entity_mv
where person_or_org = 'P' and record_status_code = 'A'
"
names <- get_cdw(names_q)

Sys.unsetenv("TZ")
Sys.unsetenv("ORA_SDTZ")

# the output file will be placed into the "/matched" subdirectory
if (!dir.exists("matched")) dir.create("matched", recursive = TRUE)
all_cads_ca %>% filter(rcpt_date >= lubridate::ymd(20140101)) %>%
    left_join(names, by = "entity_id") %>%
    select(entity_id, name, degrees, capacity, 
           tran_id, rcpt_date, amount, ctrib_dscr,
           filer_naml:sup_opp_cd) %>%
    distinct %>%
    select(-tran_id) %>%
    group_by(entity_id) %>%
    mutate(total_contributions = sum(amount)) %>%
    ungroup %>%
    arrange(desc(total_contributions), entity_id, rcpt_date) %>%
    write.csv("matched/ca_campaign.csv", row.names = FALSE)
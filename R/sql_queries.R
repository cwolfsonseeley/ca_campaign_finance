cads_names_query <- "
select distinct
entity_id,
first_name,
last_name,
middle_name
from cdw.d_bio_name_mv
where 
entity_id in (
select entity_id from cdw.d_entity_mv 
where 
person_or_org = 'P' 
and record_status_code = 'A')"

cads_address_query <- "
select distinct
entity_id,
city, 
state_code, 
zipcode, 
zipcode5
from cdw.d_bio_address_mv
where
entity_id in (
select entity_id from cdw.d_entity_mv 
where 
person_or_org = 'P' 
and record_status_code = 'A')
and contact_type_desc = 'ADDRESS'
and addr_type_code in ('H', 'B')  
and country_desc is null
"

cads_employment_query <- "
select distinct
entity_id,
job_title,
(select report_name from cdw.d_entity_mv where entity_id = d_bio_employment_mv.employer_entity_id) as employer
from cdw.d_bio_employment_mv
where
entity_id in (
select entity_id from cdw.d_entity_mv 
where 
person_or_org = 'P' 
and record_status_code = 'A')
"
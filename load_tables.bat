set PGBIN=C:\Program Files\PostgreSQL\9.3\bin\
set PGPORT=5432
set PGHOST=localhost
set PGUSER=postgres
set PGPASSWORD=postgres
set PGDATABASE=postgres
set PSQL="%PGBIN%psql"

%PSQL% -c "DROP SCHEMA IF EXISTS cal_access CASCADE;"
%PSQL% -c "CREATE SCHEMA cal_access;"
%PSQL% -c "CREATE TABLE cal_access.ca_rcpt(filing_id int not null, amend_id int not null, line_item int not null, rec_type varchar(4) not null, form_type varchar(9) not null, tran_id varchar(20), entity_cd varchar(3), ctrib_naml varchar(200), ctrib_namf varchar(45), ctrib_namt varchar(10), ctrib_nams varchar(10), ctrib_city varchar(30), ctrib_st varchar(2), ctrib_zip4 varchar(10), ctrib_emp varchar(200), ctrib_occ varchar(60), ctrib_self varchar(1), tran_type varchar(1), rcpt_date timestamp, date_thru timestamp, amount float8, cum_ytd float8, cum_oth float8, ctrib_dscr varchar(90), cmte_id varchar(9), tres_naml varchar(200), tres_namf varchar(45), tres_namt varchar(10), tres_nams varchar(10), tres_city varchar(30), tres_st varchar(2), tres_zip4 varchar(10), intr_naml varchar(200), intr_namf varchar(45), intr_namt varchar(10), intr_nams varchar(10), intr_city varchar(30), intr_st varchar(2), intr_zip4 varchar(10), intr_emp varchar(200), intr_occ varchar(60), intr_self varchar(1), cand_naml varchar(200), cand_namf varchar(45), cand_namt varchar(10), cand_nams varchar(10), office_cd varchar(3), offic_dscr varchar(40), juris_cd varchar(3), juris_dscr varchar(40), dist_no varchar(3), off_s_h_cd varchar(1), bal_name varchar(200), bal_num varchar(7), bal_juris varchar(40), sup_opp_cd varchar(1), memo_code varchar(1), memo_refno varchar(20), bakref_tid varchar(20), xref_schnm varchar(2), xref_match varchar(1), int_rate varchar(9), intr_cmteid varchar(9), PRIMARY KEY (filing_id, amend_id, line_item, rec_type, form_type));"

cat data/RCPT_CD.TSV | tr -cd '\11\12\15\40-\176' | tr -s '\r' | awk -F"\t" "NF != 63 { print }{}" > data/errorlog.tsv
cat data/RCPT_CD.TSV | tr -cd '\11\12\15\40-\176' | tr -s '\r' | awk -F"\t" "NF == 63 { print }{}" > data/clean_rcpt.tsv

%PSQL% -c "COPY cal_access.ca_rcpt from 'C:\Users\lclusr\Documents\ca_campaign_finance\data\clean_rcpt.tsv' with (format csv, delimiter E'\t', header);"

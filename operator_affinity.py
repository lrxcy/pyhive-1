#!/usr/bin/python

"""
operator_afffinity.py

Script to generate operator affinity rules that could indicate fraud by
scanning claims data.

This script needs a gpu, and needs to run on gpu0.

Expects the directory /home/db2Work to be mapped to C:\Work on 10.11.12.141
to transfer data for computation.

Script expects a table called claims_final in the specified schema with the
following columns:
    ch_claim_id
    ch_oper_id
    su_subs_id
    pa_dep_no
    ch_subm_provider_id

After running the script it will create several temp tables, and two output
tables: operator_rule_summary and operator_rule_claim_map

It will also dump a final report ranked by suspicion to operator_suspicion.csv

These tables are appended to unless the -w option is specified rather than
wiped.

"""

import ibm_db_dbi
import ibm_db
import pandas as pd
import numpy as np
import argparse as ap
from getpass import getpass
import os
import logging
from tempfile import NamedTemporaryFile
import subprocess
import re

logging.basicConfig(
    format='%(filename)s:%(levelname)s:%(asctime)s: %(message)s',
    filename="operator_affinity.log",
    level=logging.DEBUG
    # filemode='w'
)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

IGNORE_OPERATOR_SQL = '''
        ch_oper_cd       <> 'MAPPER'
    AND ch_oper_cd       <> ''
    AND ch_oper_cd not like 'WEB%'
    AND ch_oper_cd not like '%MASS_ADJUST'
    AND ch_oper_cd <> 'WEBSI'
    AND ch_oper_cd <> 'OCR'
    AND ch_oper_cd <> 'HCSA_COORD'
    AND ch_oper_cd <> 'BAYSHORE_BATCH'
'''

def load_df(df, table, cursor, columns=None):

    column_sql = ''
    if columns:
        column_sql = '(' + ', '.join(columns) + ')'
    # Put file onto db2 via smb share
    df.to_csv("/home/db2Work/gsc_temp/temp.dat", index=False,
              header=None, line_terminator='\r\n', encoding='latin-1')
    cursor.execute('''
        CALL ADMIN_CMD('LOAD FROM C://Work/gsc_temp/temp.dat OF DEL
                       INSERT INTO {0} {1}')
                   '''.format(table, column_sql))
    os.remove("/home/db2Work/gsc_temp/temp.dat")


def export_df(select, cols, cursor):

    cursor.execute('''
        CALL ADMIN_CMD('EXPORT TO C://Work/gsc_temp/temp_out.dat OF DEL
                       {0}')
                   '''.format(select.replace("'", "''")))
    df = pd.read_csv("/home/db2Work/gsc_temp/temp_out.dat", dtype=str)
    df.columns = cols
    os.remove("/home/db2Work/gsc_temp/temp_out.dat")

    return df


def init_table(schema, name, ddl, cursor,
               drop=False, like=False, truncate=True):
    if drop:
        try:
            cursor.execute("DROP TABLE {s}.{t}".format(s=schema, t=name))
        except Exception as e:
            if ibm_db.stmt_error() != '42S02':
                logger.error("Could not drop table", name)
                raise e
    try:
        if like:
            cursor.execute("CREATE TABLE {s}.{t} AS ({d}) WITH NO DATA \
                            IN TS_PD_DATA_001"
                           .format(s=schema, t=name, d=ddl))
        else:
            cursor.execute("CREATE TABLE {s}.{t} ({d}) IN TS_PD_DATA_001"
                           .format(s=schema, t=name, d=ddl))
    except Exception as e:
        if (not (ibm_db.stmt_error() == '42S01' and not drop)):
            raise e

    if (not drop and truncate):
        cursor.execute("TRUNCATE TABLE {s}.{t} IMMEDIATE"
                       .format(s=schema, t=name))


def get_unmapped_tlog(con, schema, year, month):
    cursor = con.cursor()

    select_sql = '''
    SELECT ch_claim_id
         , {c}
      FROM {s}.claims_final
     WHERE YEAR(ch_serv_dt)  =  {y}
       AND MONTH(ch_serv_dt) =  {m}
       -- These are operators we do not want to include because they are
       -- automatic systems TODO check if there are other things to ignore
       AND {i}
    '''.format(y=year, m=month, s=schema, c='{c}', i=IGNORE_OPERATOR_SQL)

    logger.info("exporting claim-providers")
    claim_prv = export_df(select_sql.format(c='ch_subm_provider_id'),
                          ['claim_id', 'provider_id'], cursor)
    logger.info("exporting claim-participants")
    claim_pat = export_df(select_sql.format(c='su_subs_id, pa_dep_no'),
                          ['claim_id', 'sub_id', 'dep_no'], cursor)
    logger.info("exporting claim-operators")
    claim_opr = export_df(select_sql.format(c='ch_oper_cd'),
                          ['claim_id', 'oper_id'], cursor)

    return pd.concat([claim_prv, claim_pat, claim_opr]).drop_duplicates()


def map_ids(umap_tlog):
    logger.info("creating entity id map")
    entity_map = umap_tlog.drop('claim_id', axis=1).drop_duplicates()
    entity_map = entity_map.reset_index(drop=True).reset_index()
    entity_map = entity_map.rename(columns={'index': 'entity_id'})
    return entity_map


def create_mapped_tlog(umap_tlog, entity_map):
    logger.info("creating mapped tlog")

    tlogs = []
    for cols in [['provider_id'], ['sub_id', 'dep_no'], ['oper_id']]:
        tlog = umap_tlog[['claim_id'] + cols].dropna()
        tlog = tlog.merge(entity_map[['entity_id'] + cols].dropna())
        tlogs.append(tlog[['claim_id', 'entity_id']])

    return pd.concat(tlogs)


def create_config(config):
    tmp_file = NamedTemporaryFile(prefix='tmp_config', suffix='.cfg', dir="./",
                                  delete=False)
    for key in config:
        if (type(config[key]) == str):
            tmp_file.write('{} = "{}"\n'.format(key, config[key]))
        else:
            tmp_file.write("{} = {}\n".format(key, config[key]))
    return tmp_file


def run_apriori(tlog, min_claims):

    logger.info("preparing apriori tmp files")
    tmp_tlog_file = NamedTemporaryFile(prefix='tmp_tlog', suffix='.dat',
                                       dir="./", delete=False)
    tmp_rule_file = NamedTemporaryFile(prefix='tmp_rule', suffix='.dat',
                                       dir="./", delete=False)
    tmp_rule_file.close()

    n_claims = tlog.claim_id.drop_duplicates().shape[0]

    tmp_cfg_file = create_config({
        'input': tmp_tlog_file.name,
        'header': 1,
        'output': '/dev/null',
        'output_smoothed': '/dev/null',
        'affin_rules_output': tmp_rule_file.name,
        'min_week': 1,
        'max_week': 1,
        'kmax': 10,
        'sup_conf_thresh': min_claims / n_claims,
        'min_sup_sample': min_claims / n_claims,
        'min_conf': 0.50,
        'smooth_window': 20,
        'cons_length': 1,
        'test': 0
    })
    tmp_cfg_file.close()

    logger.info("writing tlog for apriori")
    tlog['week'] = 1
    tlog[['week', 'claim_id', 'entity_id']]\
        .sort_values(['claim_id', 'entity_id'])\
        .to_csv(tmp_tlog_file, index=False, sep=' ')
    tmp_tlog_file.close()

    logger.info("running apriori")

    # TODO better way to get this
    apriori_bin = '/home/shared_folder/dpeterson_git/Apriori/apriori'
    subprocess.call([apriori_bin, tmp_cfg_file.name])

    logger.info("apriori completed, reading results")
    rules = pd.read_csv(tmp_rule_file.name, header=None)
    rules.columns = ['week', 'text', 'sup', 'conf', 'lift', 'precedent',
                     'consequent']

    os.remove(tmp_tlog_file.name)
    os.remove(tmp_rule_file.name)
    os.remove(tmp_cfg_file.name)

    return rules


def create_vertical_rules(rules):
    logger.info("creating vertical rules dataframe")

    prec = np.delete(np.char.array(rules.precedent).partition('+'), 1, axis=1)
    cons = np.delete(np.char.array(rules.consequent).partition('+'), 1, axis=1)

    umap_rules = pd.DataFrame(columns=['p1', 'p2', 'c1', 'c2', 'p', 'c',
                                       'sup', 'conf', 'lift'])
    umap_rules.p1 = prec[:, 0]
    umap_rules.p2 = prec[:, 1]
    umap_rules.c1 = cons[:, 0]
    umap_rules.c2 = cons[:, 1]
    umap_rules.sup = rules.sup
    umap_rules.conf = rules.conf
    umap_rules.lift = rules.lift
    umap_rules.p = 'p'
    umap_rules.c = 'c'

    umap_rules = umap_rules.reset_index().rename(columns={'index': 'rule_id'})
    vrules = pd.DataFrame(columns=['rule_id', 'entity_id', 'pc', ])
    vrules_parts = [
        umap_rules[['rule_id', 'p1', 'p', 'sup', 'conf', 'lift']],
        umap_rules[['rule_id', 'p2', 'p', 'sup', 'conf', 'lift']],
        umap_rules[['rule_id', 'c1', 'c', 'sup', 'conf', 'lift']],
        umap_rules[['rule_id', 'c2', 'c', 'sup', 'conf', 'lift']]
    ]
    for part in vrules_parts:
        part.columns = ['rule_id', 'entity_id', 'pc', 'sup', 'conf', 'lift']
    vrules = pd.concat(vrules_parts).sort_values('rule_id')

    vrules = vrules[vrules.entity_id != ''].reset_index(drop=True)
    vrules.entity_id = pd.to_numeric(vrules.entity_id, errors='coerse')
    return vrules


def unmap_vertical_rules(vrules, emap):
    logger.info("unmapping rules")

    return vrules.merge(emap, on='entity_id')


def load_rules_to_db(con, schema, unmap_vrules, year, month, wipe):
    logger.info("uploading rules to db")

    cursor = con.cursor()
    init_table(schema, 'operator_rules', '''
               rule_id      bigint,
               year         integer,
               month        integer,
               pc           char(1),
               sup          float,
               conf         float,
               lift         float,
               oper_id      varchar(30),
               provider_id  bigint,
               su_subs_id   bigint,
               pa_dep_no    varchar(2)
               ''',
               cursor, drop=False or wipe, truncate=False)

    get_max_rule_id_sql = '''
    SELECT COALESCE(MAX(rule_id), 1)
    FROM {s}.operator_rules
    '''.format(s=schema)
    max_rule_id = pd.read_sql(get_max_rule_id_sql, con).values[0, 0]
    unmap_vrules.rule_id = unmap_vrules.rule_id + max_rule_id

    unmap_vrules['year'] = year
    unmap_vrules['month'] = month
    load_df(unmap_vrules[['rule_id', 'year', 'month', 'pc', 'sup', 'conf',
                          'lift', 'oper_id', 'provider_id', 'sub_id',
                          'dep_no']],
            '{}.operator_rules'.format(schema), cursor)


def get_args():
    parser = ap.ArgumentParser(
        description='Operator affinity analysis for fraud detection')
    parser.add_argument("-s", "--schema", type=str,
                        help="schema from which to pull claims data")
    parser.add_argument("-y", "--year", type=int,
                        help="year for which to pull claims data")
    parser.add_argument("-m", "--month", type=int,
                        help="month from which to pull claims data")
    parser.add_argument("-u", "--user", type=str,
                        help="username on 10.11.12.101 for access to db2")
    parser.add_argument("-p", "--password", type=str,
                        help="password")
    parser.add_argument("-w", "--wipe", action='store_true',
                        help="wipe older month data?")
    parser.add_argument("-r", "--gen_report", action='store_true',
                        help="just generate the final report")
    parser.add_argument("-c", "--claims", type=float, default=10.0,
                        help="minimum number of claims for apriori sup threshold")
    parser.add_argument("--claims_for", type=str,
                        help="pull back claims for a specific rule description")
    args = parser.parse_args()

    if not args.schema:
        args.schema = 'gsc_dental'

    if ((args.gen_report or args.claims_for) and not args.year):
        logger.error('Need to provide year')
        exit(1)
    elif not ((args.month and args.year) or args.gen_report or args.claims_for):
        logger.error('Need to provide month and year')
        exit(1)

    if not args.user:
        logger.error('Need a db2 username (-u)')
        exit(1)

    if not args.password:
        args.password = getpass()

    # logger.debug(str(args))
    return args


def create_rule_desc_strings(con, schema, year, month):
    cursor = con.cursor()

    init_table(schema, 'operator_rule_desc', '''
               rule_id      bigint,
               year         integer,
               month        integer,
               sup          float,
               conf         float,
               lift         float,
               description  varchar(1000)
               ''', cursor, drop=True, truncate=False)

    desc_insert_sql = '''
    INSERT INTO {s}.operator_rule_desc
    WITH
    concat_provider_mapping AS (
        SELECT new_provider_id,
        SUBSTR(XMLCAST(XMLGROUP('+' || pr_provider_id AS a ORDER BY pr_provider_id) AS VARCHAR(1000)), 2) as real_providers
        --TODO fix this so the schema is dynamic
        FROM gsc_network.provider_mapping pm
        GROUP BY new_provider_id

    ),
    rule_row_strings AS (
        SELECT rule_id, year, month, pc, sup, conf, lift,
            '[' || TRIM(COALESCE(varchar(max(oper_id)), '') || ' ' ||
                        COALESCE(varchar(max(real_providers)), '') || ' ' ||
                        COALESCE(varchar(max(su_subs_id)) || '-' ||
                                    max(pa_dep_no), '')) || ']'
            as string,
            CASE WHEN max(oper_id) IS NULL THEN 0 ELSE 1 END AS is_oper
        FROM {s}.operator_rules opr
        LEFT JOIN concat_provider_mapping pm ON (
            opr.provider_id = pm.new_provider_id
        )
        GROUP BY rule_id, year, month, pc, sup, conf, lift
        HAVING max(oper_id) IS NULL OR count(*) = 1
    )
    SELECT p.rule_id, p.year, p.month, p.sup, p.conf, p.lift,
           p.string || ' ==> ' || c.string
    FROM rule_row_strings p, rule_row_strings c
    WHERE p.rule_id  = c.rule_id
      AND p.pc = 'p'
      AND c.pc = 'c'
      AND (p.is_oper = 1 or c.is_oper = 1)
    '''.format(s=schema)
    logger.debug(desc_insert_sql)
    logger.info("creating rule description strings")
    cursor.execute(desc_insert_sql)


def summerize_operator_rules(con, schema, year, month, wipe):
    cursor = con.cursor()

    logger.info("summarizing operator rules")

    init_table(schema, 'operator_rule_claim_map_tmp', '''
               rule_id      bigint,
               claim_id     bigint
               ''', cursor, drop=True, truncate=False)

    init_table(schema, 'operator_rule_claim_map', '''
               rule_id      bigint,
               claim_id     bigint
               ''', cursor, drop=False or wipe, truncate=False)

    init_table(schema, 'operator_rule_summary', '''
               rule_id      bigint,
               year         integer,
               month        integer,
               description  varchar(1000),
               sup          float,
               conf         float,
               lift         float,
               n_claims     integer,
               net_paid     decimal(10,2)
               ''', cursor, drop=False or wipe, truncate=False)

    init_table(schema, 'operator_rule_collections', '''
               rule_id      bigint,
               provider_type varchar(20),
               client_id    bigint,
               benefit_type varchar(20)
               ''', cursor, drop=False, truncate=False)


    tmp_map_insert_sql = '''
    INSERT INTO {s}.operator_rule_claim_map_tmp
    SELECT DISTINCT opr.rule_id, cf.ch_claim_id
    FROM {s}.claims_final cf,
         {s}.operator_rules opr
    WHERE YEAR(cf.ch_serv_dt) = {y}
      AND MONTH(cf.ch_serv_dt) = {m}
      AND opr.year = {y}
      and opr.month = {m}
      AND {j}
    '''.format(s=schema, y=year, m=month, j='{j}')

    logger.info("finding claims associated with rules")
    for join_condition in ['cf.ch_oper_cd = opr.oper_id',
                           'cf.ch_subm_provider_id = opr.provider_id',
                           'cf.su_subs_id = opr.su_subs_id '
                           'AND cf.pa_dep_no = opr.pa_dep_no']:
        logger.debug(tmp_map_insert_sql.format(j=join_condition))
        cursor.execute(tmp_map_insert_sql.format(j=join_condition))

    tmp_map_null_fix_sql = '''
    INSERT INTO {s}.operator_rule_claim_map_tmp
    SELECT DISTINCT a.rule_id
         , b.claim_id
      FROM (
            SELECT rule_id
            FROM {s}.operator_rules
            GROUP BY rule_id
            HAVING count(*)  <= 2) a
         , {s}.operator_rule_claim_map_tmp b
     WHERE a.rule_id =  b.rule_id
    '''.format(s=schema)

    logger.debug(tmp_map_null_fix_sql)
    cursor.execute(tmp_map_null_fix_sql)

    rule_map_sql = '''
    INSERT INTO {s}.operator_rule_claim_map
    SELECT rule_id, claim_id
    FROM {s}.operator_rule_claim_map_tmp
    GROUP BY rule_id, claim_id
    HAVING count(*) >= 3
    '''.format(s=schema)

    logger.debug(rule_map_sql)
    cursor.execute(rule_map_sql)

    logger.info("Creating final summary")
    rule_summary_sql = '''
    INSERT INTO {s}.operator_rule_summary
    SELECT rm.rule_id, rd.year, rd.month,
           rd.description, rd.sup, rd.conf, rd.lift,
           count(distinct cf.ch_claim_id), sum(ch_paid_cdn_amt)
    FROM {s}.claims_final cf,
         {s}.operator_rule_claim_map rm,
         {s}.operator_rule_desc rd
    WHERE cf.ch_claim_id = rm.claim_id
      AND rm.rule_id = rd.rule_id
      AND rd.year = {y}
      AND rd.month = {m}
    GROUP BY rm.rule_id, rd.year, rd.month, rd.description,
        rd.sup, rd.conf, rd.lift
    '''.format(s=schema, y=year, m=month)

    logger.debug(rule_summary_sql)
    cursor.execute(rule_summary_sql)

    collection_sql = '''
    INSERT INTO {s}.operator_rule_collections
    SELECT rm.rule_id
         , cf.ch_provider_type_cd
         , cf.ch_client_id
         , benefit_type_from_proc_code(cf.benefit_cat, cf.ch_proc_din_no)
    FROM {s}.claims_final cf
       , {s}.operator_rule_claim_map rm
    WHERE cf.ch_claim_id = rm.ch_claim_id
    '''.format(s=schema)

    logger.debug(collection_sql)
    cursor.execute(collection_sql)


def get_operator_stats(con, schema, year):
    provinces = ['AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE',
                 'QC', 'SK', 'YT']
    operator_stats_sql = '''
    SELECT ch_oper_cd
         , count(distinct ch_claim_id) as n_claims
         , sum(ch_paid_cdn_amt) as net_paid
         , count(distinct ch_subm_provider_id) as n_providers
         , count(distinct su_subs_id || '-' || pa_dep_no) as n_participants
      FROM {s}.claims_final
      WHERE year(ch_serv_dt) = {y}
        AND {i}
      GROUP BY ch_oper_cd
      ORDER BY n_claims DESC
    '''.format(s=schema, y=year, i=IGNORE_OPERATOR_SQL)
    logger.debug(operator_stats_sql)

    op_stats = pd.read_sql(operator_stats_sql, con)
    op_stats['N_CLAIMS_PCT'] = op_stats['N_CLAIMS'].rank(pct=True).round(4) * 100
    op_stats['N_PARTICIPANTS_RANK'] = op_stats['N_PARTICIPANTS'].rank(ascending=False)

    op_stats.to_csv("operator_stats.csv", index=False)


def create_suspiscion_ranking(con, schema, year):
    get_suspicion_ranks_sql = '''
    SELECT
        description,
        count(*) as n_months,
        sum(sup) as sup,
        sum(conf * n_claims) / sum(n_claims) as avg_conf,
        avg(lift) as avg_lift,
        sum(n_claims) as n_claims,
        sum(net_paid) as net_paid,
        sum(n_claims * conf) as suspicion,
    FROM
        {s}.operator_rule_summary rs,
        {s}.operator_rule_collections rc
    WHERE year = {y}
    GROUP BY description
    HAVING sum(net_paid) > 0
    ORDER BY
        suspicion desc,
        net_paid desc
    '''.format(s=schema, y=year)

    logger.debug(get_suspicion_ranks_sql)

    suspicion_ranks = pd.read_sql(get_suspicion_ranks_sql, con)
    suspicion_ranks['SUSPICION'] = suspicion_ranks['SUSPICION'].rank(pct=True).round(4) * 1000

    suspicion_ranks.to_csv("operator_suspicions.csv", index=False)


def get_average_max_confidence(con, schema, year):
    avg_conf_sql = '''
    WITH sups AS (
        SELECT ch_subm_provider_id
             , ch_oper_cd
             , count(distinct ch_claim_id) as sup
        FROM gsc_dental.claims_final
        WHERE year(ch_serv_dt) = {y}
          AND {i}
        GROUP BY ch_subm_provider_id
    )
    SELECT ch_sum_provider_id, ch_oper_cd,
    sup / sum(sup) over (partition by ch_subm_provider_id) as conf
    FROM sups
    '''.format(s=schema, i=IGNORE_OPERATOR_SQL, y=year)

    avg_conf = pd.read_sql(avg_conf_sql, con)
    print avg_conf
    return avg_conf


def claims_for_desc(con, schema, desc):
    get_claim_sql = '''
    SELECT DISTINCT rd.DESCRIPTION
         , cf.*
      FROM {s}.claims_final cf
         , {s}.OPERATOR_RULE_CLAIM_MAP rm
         , {s}.OPERATOR_RULE_SUMMARY rd
     WHERE rd.DESCRIPTION LIKE '{d}'
       AND rd.RULE_ID                        =  rm.rule_id
       AND rm.claim_id                       =  cf.CH_CLAIM_ID
       AND year(ch_serv_dt) = 2017
       '''.format(s=schema, d=desc)

    print get_claim_sql
    filename = re.sub("[\[\] =>]", "", re.sub(" ", "+", re.sub(" ==> ", "-", desc)))
    pd.read_sql(get_claim_sql, con).to_csv(filename + ".csv", index=False)


def create_report_files(con, schema, year):
    logger.info("generating suspicion report")

    #get_average_max_confidence(con, schema, year)
    create_suspiscion_ranking(con, schema, year)
    get_operator_stats(con, schema, year)


def main():
    args = get_args()

    logger.info("connecting to DB")
    con = ibm_db_dbi.connect("DATABASE=wm;HOSTNAME=10.11.12.141;PORT=50000;"
                             "PROTOCOL=TCPIP;"
                             "UID={}; PWD={};".format(args.user, args.password))
    con.set_autocommit(True)

    try:
        if args.gen_report:
            create_report_files(con, args.schema, args.year)
            exit(0)

        if args.claims_for:
            claims_for_desc(con, args.schema, args.claims_for)
            exit(0)

        # umap_tlog = get_unmapped_tlog(con, args.schema, args.year, args.month)
        # entity_map = map_ids(umap_tlog)
        # mapped_tlog = create_mapped_tlog(umap_tlog, entity_map)
        # rules = run_apriori(mapped_tlog, args.claims)
        # vrules = create_vertical_rules(rules)
        # unmap_vrules = unmap_vertical_rules(vrules, entity_map)
        # load_rules_to_db(con, args.schema, unmap_vrules, args.year, args.month,
        #                  args.wipe)
        create_rule_desc_strings(con, args.schema, args.year, args.month)
        summerize_operator_rules(con, args.schema, args.year, args.month,
                                 args.wipe)
        create_report_files(con, args.schema, args.year)
    finally:
        logger.info("Closing db connection")
        con.close()


if __name__ == "__main__":
    main()

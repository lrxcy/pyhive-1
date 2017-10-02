#!/usr/bin/python

import ibm_db_dbi
import ibm_db
import pandas as pd
import logging
import os
from tempfile import NamedTemporaryFile

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

# Used by the load_df and export_df functions to transfer dat files for loading
# to db2
TRANSFER_DIR_UNIX = '/home/db2Work/gsc_temp/'
TRANSFER_DIR_WIN = 'C:\\Work\\gsc_temp\\'


def load_df(df, schema, table, con, columns=None):
    cursor = con.cursor()

    tmp_file = NamedTemporaryFile(suffix='.dat', dir="./", delete=True)
    tmp_file.close()
    tmp_file = os.path.basename(tmp_file.name)

    column_sql = ''
    if columns:
        column_sql = '(' + ', '.join(columns) + ')'

    # Put file onto db2 via smb share
    df.to_csv(TRANSFER_DIR_UNIX + tmp_file, index=False,
              header=None, line_terminator='\r\n', encoding='latin-1', sep='|')
    try:
        cursor.execute('''
            CALL ADMIN_CMD('LOAD FROM ''{p}/{f}'' OF DEL MODIFIED BY COLDEL|
                           timestampformat="MM/DD/YYYY HH:MM:SSTT"
                           INSERT INTO {s}.{t} {c}')
                    '''.format(s=schema,
                               t=table,
                               c=column_sql,
                               p=TRANSFER_DIR_WIN,
                               f=tmp_file
                               ))
    except Exception as e:
        raise e
    finally:
        os.remove(TRANSFER_DIR_UNIX + tmp_file)

    cursor.close()


def export_df(select, cols, con):
    cursor = con.cursor()

    tmp_file = NamedTemporaryFile(suffix='.dat', dir="./")
    tmp_file.close()
    tmp_file = os.path.basename(tmp_file.name)

    cursor.execute('''
        CALL ADMIN_CMD('EXPORT TO {p}/{f} OF DEL
                       {s}')
                   '''.format(s=select.replace("'", "''"),
                              p=TRANSFER_DIR_WIN,
                              f=tmp_file))

    try:
        df = pd.read_csv(TRANSFER_DIR_UNIX + tmp_file,
                         encoding='latin-1', dtype=str, header=None)
        df.columns = cols
    finally:
        os.remove(TRANSFER_DIR_UNIX + tmp_file)

    cursor.close()

    return df


def init_table(schema, name, ddl, con,
               drop=False, like=False, truncate=True,
               part_keys=None):
    cursor = con.cursor()

    logger.debug("Initing table {s}.{n} with options: "
                 "drop={d}, like={l}, truncate={t}".format(
                    s=schema, n=name, d=drop, l=like, t=truncate))

    if drop:
        try:
            cursor.execute("DROP TABLE {s}.{t}".format(s=schema, t=name))
        except Exception as e:
            if ibm_db.stmt_error() != '42S02':
                logger.error("Could not drop table", name)
                raise e

    if part_keys:
        part_keys_sql = "DISTRIBUTE BY HASH (" + ",".join(part_keys) + ")"
    else:
        part_keys_sql = ''

    try:
        if like:
            cursor.execute("CREATE TABLE {s}.{t} AS ({d}) WITH NO DATA \
                            IN TS_PD_DATA_001 {pk}"
                           .format(s=schema, t=name, d=ddl, pk=part_keys_sql))
        else:
            cursor.execute("CREATE TABLE {s}.{t} ({d}) IN TS_PD_DATA_001 {pk}"
                           .format(s=schema, t=name, d=ddl,
                                   pk=part_keys_sql))
    except Exception as e:
        if (not (ibm_db.stmt_error() == '42S01' and not drop)):
            raise e

    if (not drop and truncate):
        cursor.execute("TRUNCATE TABLE {s}.{t} IMMEDIATE"
                       .format(s=schema, t=name))
    cursor.close()


def get_db2_con(user, password):
    logger.info("Connecting to db2")
    con = ibm_db_dbi.connect("DATABASE=wm;HOSTNAME=10.11.12.141;PORT=50000;"
                             "PROTOCOL=TCPIP;"
                             "UID={}; PWD={};".format(user, password))
    con.set_autocommit(True)
    return con

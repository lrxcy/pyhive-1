""""Functionality
1) Create table from dataframe
2) Insert into table from dataframe
3) Update table from dataframe (specify intersection columns)
4) create table, drop table, check if exists
5) Wrapper function to do the above from a csv


Error Handling
1) What to do if update columns are different than table columns
2) If table doesn't exist and you try to do an update
"""
import pandas as pd
import numpy as np
import os
from pyhive import hive
import re

def create_database(database, con):
    """
    Creates a database if it doesn't exist
    """
    cursor = con.cursor()
    qry = """
    CREATE DATABASE IF NOT EXISTS {database}
    """.format(database=database)
    cursor.execute(qry)
    return None

def drop_table(database, table_name, con):
    """
    Drop a table from database.
    INPUTS: database and table name as well as connection
    """
    cursor = con.cursor()
    cursor.execute("""
        drop table if exists {database}.{table_name}
        """.format(database = database, table_name = table_name))

def rename_table(database, old_name, new_name, con):
    """
    Renames a table from 'old_name' to 'new_name'
    """
    cursor=con.cursor()
    qry="""
    ALTER TABLE {database}.{old_name} RENAME TO {database}.{new_name}
    """.format(database=database, old_name=old_name, new_name=new_name)
    cursor.execute(qry)
    return None

def create_table(database, table_name, ddl_string, con):
    """
    Create a table into database.
    INPUTS: name of the database, name of the target table, ddl for table and connection
    """
    recreate_table_query = """
        create table if not exists {database}.{table_name}
        ({ddl}) stored as orc
        """.format(database = database, table_name = table_name, ddl=ddl_string)
    cursor = con.cursor()
    cursor.execute(recreate_table_query)

def table_exists(database, table_name, con):
    """
    Checks if the table exists
    Returns True is table exists, False otherwise
    """
    qry = 'show tables in {database} like "{table_name}"'.format(
        database=database,
        table_name=table_name
    )
    is_empty = pd.read_sql(qry, con).empty
    if not is_empty: return True
    return False


def create_table_from_df(X, DB, table_name, con, hive_dtypes={}):
    """
    Load a table to hive. Infer dtypes unless explicitely specified

    Parameters:
    X - df to load
    DB - database to load into
    table_name - table to create
    con - hive connection from connect_to_hive
    hive_dtypes - dictionary mapping column names to hive dtypes. Will override
                  this function's dtype inferences
    """
    cursor = con.cursor()
    type_map = {
        'float64': 'float',
        'int64': 'bigint',
        'string': 'string',
        'object': 'string',
        'int16': 'smallint',
        'datetime64[ns]': 'date'
    }
    ddl = X.dtypes.replace(type_map).to_string()
    ddl = ','.join(ddl.split('\n'))
    for col in hive_dtypes:
        ddl = re.sub(col + "\s+\w+", col + " " + hive_dtypes[col], ddl)

    # Drop / Recreate table
    drop_table(DB, table_name, con)
    create_table(DB, table_name, ddl, con)

    # Create temp table to load local data into
    txt_table = table_name + '_txt'
    drop_table(DB, txt_table, con)
    create_tmp_table = """
    CREATE TABLE {database}.{table_name} {col_types}
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    """.format(
        database=DB,
        table_name=txt_table,
        col_types='('+ddl+')'
    )
    cursor.execute(create_tmp_table)
    # Load data into temp table
    temp_file = 'temp_table.csv'
    X.to_csv(temp_file, index=False, header=None)
    cwd = os.path.join(os.getcwd(), temp_file)
    load_string = """
    LOAD DATA LOCAL INPATH '{csv_path}'
    INTO TABLE {database}.{table_name}
    """.format(
        csv_path=cwd,
        database=DB,
        table_name=txt_table
    )
    cursor.execute(load_string)
    # Insert into the ORC table
    insert_string = """
    INSERT OVERWRITE TABLE
    {database}.{orc_table}
    SELECT * FROM {database}.{tmp_table}
    """.format(
        database=DB,
        orc_table=table_name,
        tmp_table=txt_table
    )
    cursor.execute(insert_string)

    # Drop the temp table
    drop_table(DB, txt_table, con)

    # Run analyze on the new table
    analyze_table(DB, table_name, con)

    # Delete the csv file
    if os.path.isfile(cwd):
        os.remove(cwd)
    return None

def insert_update(df, union_cols, database, base_table, con, mode='update'):
    """
    If table exists, then perform update steps for table
    If table not exists, then create table from df instead

    Update steps:
    1) Make backup copy of base table
    2) Put df into a incremental table
    3) Take the union of base table with incremental table on 'union_cols'
    4) Insert 3) into table
    """

    # Define table names
    inc_table = base_table + '_incremental'
    new_table = base_table + '_new'
    # Check for existence of table
    if not table_exists(database, base_table, con):
        print "No table found to update, writing incremental into a new table"
        create_table_from_df(df, database, base_table, con)
        return None

    if mode=='update':
        # UPDATE left joins then unions the incremental table
        left_or_right = 'LEFT'
        in_or_up = inc_table
        print "Performing Update into {table}".format(table=base_table)
    elif mode=='insert':
        # INSERT right joins then unions the base table
        left_or_right = 'RIGHT'
        in_or_up = base_table
        print "Performing Insert into {table}".format(table=base_table)
    else:
        print "Improper mode in insert_update, use either 'update' or 'insert'"
        sys.exit()
    # Define the hive cursor
    cursor = con.cursor()
    # Make copy of base table
    # Create temp table for incremental
    create_table_from_df(df, database, inc_table, con)
    # Take the intersection of the tables on union_cols
    # String to select the joined columns with alias
    select_cols = ', '.join(['a.'+c+' as '+c for c in df.columns])
    # Strings for the aliased a and b columns
    acols = ['a.'+a for a in union_cols]
    bcols = ['b.'+b for b in union_cols]
    # String to create the 'ON' condition
    join_cols = ' AND '.join([a+'='+b for a,b in zip(acols, bcols)])
    # String to create the 'WHERE' condition
    is_null = ' IS NULL AND '.join(bcols) + ' IS NULL'
    # Drop the new table if exists
    drop_table(database, new_table, con)
    qry="""
    CREATE TABLE {database}.{new_table} STORED AS ORC AS
    WITH new_base AS(
        SELECT
            {a_alias}
        FROM
            {database}.{base} a
        {how} JOIN
            {database}.{incremental} b
        ON
            ({join_cols})
        WHERE
            ({is_null})
    ),
    table_union AS(
        SELECT {cols} FROM {database}.{in_or_up}
        UNION
        SELECT {cols} FROM new_base
    )
    SELECT {cols} FROM table_union
    """.format(
        in_or_up=in_or_up,
        how=left_or_right,
        a_alias=select_cols,
        database=database,
        base=base_table,
        incremental=inc_table,
        new_table=new_table,
        join_cols=join_cols,
        is_null=is_null,
        cols=', '.join(df.columns.tolist())
    )
    cursor.execute(qry)

    # QA table before dropping the original and renaming
    base_keys = pd.read_sql("""
    SELECT DISTINCT {cols} FROM {database}.{table}
    """.format(
        database=database,
        table=base_table,
        cols=', '.join(union_cols)
        ), con)
    inc_keys = df[union_cols].drop_duplicates()
    new_keys = pd.read_sql("""
    SELECT DISTINCT {cols} FROM {database}.{table}
    """.format(
        database=database,
        table=new_table,
        cols=', '.join(union_cols)
        ), con)
    expected_number = base_keys.append(inc_keys).drop_duplicates().shape[0]
    actual_number = new_keys.shape[0]
    print expected_number, actual_number

    # Drop the old base table and rename the new one
    drop_table(database, base_table, con)
    rename_table(database, new_table, base_table, con)
    # Clean up the tables
    drop_table(database, new_table, con)
    drop_table(database, inc_table, con)
    # Analyze the new table
    analyze_table(database, base_table, con)
    return None

def connect_to_hive(hive_user):
    hive_host = '10.11.12.144'
    hive_conf = {'job.queue.name': 'default'}
    return hive.Connection(host=hive_host, username=hive_user,configuration=hive_conf)

def analyze_table(database, table_name, con):
    cursor = con.cursor()
    cursor.execute("""
        ANALYZE TABLE {database}.{table_name}
        COMPUTE STATISTICS FOR COLUMNS
        """.format(database = database,
                   table_name = table_name))


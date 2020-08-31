import os,sys
ospath = os.path.dirname(os.path.abspath(__file__))+'/python/lib/python3.7/site-packages'
sys.path.insert(1,ospath)
import MySQLdb
import pandas as pd
import numpy as np
import pandas_gbq
import hashlib


def get_credentials(basedir,fname):
    """Gets the credentials for the mysql db."""
    with open(os.path.join(basedir, fname), 'r') as f:
        return list(map(str.strip, f.readlines()))

def getTableList(basedir,fname):
    """Gets the tables to migrated from File"""
    with open(os.path.join(basedir, fname), 'r') as f:
        return list(map(str.strip, f.readlines()))

def read_data(conn, table_name, query=None):
    """Reads the given table from the mysql db."""
    print(f'Querying {table_name}...')
    if query is None:
        return pd.read_sql(f'select * from {table_name}', conn)
    else:
        return pd.read_sql(query, conn)

def upload_data(data, destination_table, if_exists='replace'):
    """Uploads the data to the BigQuery."""
    print(f'Uploading {destination_table} data to BigQuery...')
    pandas_gbq.to_gbq(data, project_id='dotted-marking-256715', destination_table=destination_table,
                      if_exists=if_exists)

def configureGCPCred(basedir,fname):
    flname = os.path.join(basedir,fname)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = flname
    return "ENV Set!"

if __name__ == '__main__':
    basedir = os.path.dirname(os.path.realpath(__file__))
    db, host, user, passwd = get_credentials(basedir,'mysql_credentials.txt')
    db_adept_connect = MySQLdb.connect( host=host, db=db, user=user, passwd=passwd )
    gcpCredSet = configureGCPCred(basedir,'srvkey.json')
    tableLst = getTableList(basedir,'tables.txt')

    collection = 'adeptTempTest'
    for table_name in tableLst:
        #table_name = 'wp_popularpostsdata'
        tableData = read_data(db_adept_connect,table_name)
        destination_table = f'{collection}.{table_name}'
        upload_data = upload_data(tableData,destination_table)

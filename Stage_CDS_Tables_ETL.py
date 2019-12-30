
# coding: utf-8

# In[1]:


## import the necessaries
from __future__ import division
from __future__ import print_function
import pandas as pd
import keyring
import getpass
from time import sleep
import numpy as np
from impala.dbapi import connect
import subprocess
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
import os


# #### Parameters

# In[2]:


# make impyla connection to run action queries thru impala interface
def impala_conn():
    return connect(host='poldcdhdn010.dev.intranet', 
                   port=21050,
                   use_ssl=True,
                   auth_mechanism='GSSAPI')
impala = create_engine('impala://', creator=impala_conn, poolclass=NullPool) 


# ## Setup Pysqoop function - altered from Stephen Quinn's original

# In[3]:


# sqoops oracle table into CDL. Expected params: tablename,schema, hdfs_dir
# example: tableaname='motive68', schema='bdalab_sa', 
# pysqoop(tablename=tablename, schema=schema, hdfs_dir=hdfs_dir)
# Optional param: merge_key (string, used like a primary key for upserts, oracle_query
# Optional params: oracle_query (select statement from oracle. only supports one where clause)
def pysqoop(**kwargs):
    '''
    This takes an oracle query and sqoops the result into the dev DataLake. 
    keyword args:
        tablename - string - name of table in Oracle and in the datalake
        schema - string - schema for the datalake table being created/inserted into
        oracle_schema - string - schema, if different, for the oracle table being sqooped
        hdfs_dir - 
    '''
    tablename = kwargs.get('tablename')
    schema = kwargs.get('schema')
    oracle_schema = kwargs.get('oracle_schema','')
    hdfs_dir = kwargs.get('hdfs_dir', '/data/CTL/encrypt/db/')
    merge_key = kwargs.get('merge_key', '')
    is_impala = kwargs.get('is_impala', False)
    is_reload = kwargs.get('is_reload',False)
    # to do - this would be good to give some of this as a list from our json file at some point
    oracle_host = kwargs.get('oracle_host','pddclodm-scan.corp.intranet')
    oracle_port = kwargs.get('oracle_port','1521')
    oracle_service = kwargs.get('oracle_service','CCDW01PU1')
    oracle_query = kwargs.get('oracle_query')
    split_column = kwargs.get('splitby',None)
    usr = kwargs.get('usr',getpass.getuser())
    pwd = kwargs.get('pwd',keyring.get_password('CCDW01PU1', usr))

#     for key in kwargs:
#         print(kwargs[key])
    if oracle_schema and not oracle_query:
        oracle_query= "SELECT * FROM {}.{}".format(oracle_schema,tablename)
    elif not oracle_query:
        oracle_query = "SELECT * FROM {}.{}".format(usr,tablename)
    # add oracle schema to tablename for DL if present
    if oracle_schema:
        tablename = "{}_{}".format(oracle_schema,tablename)
        
    target_path =  '{}{}/{}_stage'.format(hdfs_dir, schema, tablename)
    target_table = '{}.{}'.format(schema,tablename)
    stage_path = '{}{}/{}_temp'.format(hdfs_dir, schema, tablename)
    stage_table = target_table+'_work'
    # clean data and format
    oracle_path = 'jdbc:oracle:thin:@//{}:{}/{}'.format(oracle_host,oracle_port,oracle_service)
    if '$CONDITIONS' not in oracle_query:
        if 'where' in oracle_query.lower():
            oracle_query = '"' + oracle_query + ' and \$CONDITIONS"'
        else:
            oracle_query = '"' + oracle_query + ' where \$CONDITIONS"'
    oracle_query = oracle_query.replace('\n', ' ').replace('\r', '').replace('"','')   
    print(oracle_query)
    # drop stage table if it exists
    
    invalidate_meta = "INVALIDATE METADATA {}"
    refresh_table = "REFRESH {}"
    drop_table = "drop table if exists {} purge"
    
    if is_impala:
        try:
            impala_engine = create_engine('impala://', creator=impala_conn, poolclass=NullPool)
            impala = impala_engine # to-do: replace impala_engine with impala
            cdl_tables = pd.read_sql("show tables in {}".format(schema), impala_engine).name.tolist()
            try:
                impala.execute(invalidate_meta.format(stage_table))
                impala.execute(refresh_table.format(stage_table))
                impala.execute(drop_table.format(stage_table))
            except Exception as e:
#                 print("try 2.0 failed",e)
                pass
        except Exception as e: # if impala doesn't work, switch to hive/spark
#             print("try 1.0 failed",e)
            try:
                print('trying spark/hive...')
                #is_impala = False
                cdl_tables = sqlContext.sql("show tables in {}".format(schema)).toPandas()
                cdl_tables = cdl_tables.tableName.tolist()
                sqlContext.sql(drop_table.format(stage_table))
            except Exception as e:
                #print(e)
#                 print("try 1 then 2.5 failed",e)
                pass
    else:
        print('is_impala=False, trying spark/hive...')
        cdl_tables = sqlContext.sql("show tables in {}".format(schema)).toPandas()
        cdl_tables = cdl_tables.tableName.tolist()
        sqlContext.sql(drop_table.format(stage_table))
    
    """
    try:
        if is_impala:
            try:
                try:
                    impala.execute(invalidate_meta.format(stage_table))
                    impala.execute(refresh_table.format(stage_table))
                except:
                    pass
                impala.execute(drop_table.format(stage_table))
                cdl_tables = pd.read_sql("show tables in {}".format(schema), impala_engine).name.tolist()
            except Exception as e: # if impala doesn't work, switch to hive/spark
                print(e)
                is_impala = False
                sqlContext.sql(drop_table.format(stage_table))
        else:
            sqlContext.sql("drop table if exists {} purge".format(stage_table))
    except Exception as e:
        pass
    """
    
    
    # Setup Scoop
    if split_column:
        sqoop_this =r'''sqoop import --connect {0} --username {1} --password {2} --as-textfile --query "{3}" --target-dir {4} --delete-target-dir --fields-terminated-by '\0x7C' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' --split-by {5} --num-mappers 4 --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-drop-import-delims --create-hive-table -hive-table {6} --verbose;'''.format(oracle_path,usr,pwd,oracle_query,stage_path,split_column,stage_table)
    else:
        sqoop_this =r'''sqoop import --connect {0} --username {1} --password {2} --as-textfile --query "{3}" --target-dir {4} --delete-target-dir --fields-terminated-by '\0x7C' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' -m 1 --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-drop-import-delims --create-hive-table -hive-table {5} --verbose;'''.format(oracle_path,usr,pwd,oracle_query,stage_path,stage_table)
    r = os.system(sqoop_this)
    
    
    try:
        if r == 0:
            print('Sqooping was successful. Processing {} table in the datalake'.format(stage_table))
            
            # set up queries
            if merge_key:
                merge_query = """
                        INSERT INTO {0}
                        SELECT DISTINCT *
                        FROM
                          ( SELECT *
                            FROM {1}
                            WHERE {2} NOT IN
                               (SELECT {2}
                                FROM {0}) 
                         ) WORK
                """.format(target_table,stage_table, merge_key)
            else:
                merge_query = """ SELECT DISTINCT * FROM {0} """.format(stage_table)     
        
            create_query = """CREATE TABLE {0} stored as parquet as SELECT DISTINCT * FROM {1}""".format(target_table,stage_table)
            #impala.execute('invalidate metadata {}'.format(stage_table))
            if tablename.lower() not in cdl_tables:
                print('creating table {}'.format(target_table))
                if is_impala:
                    try:
                        impala.execute(invalidate_meta.format(stage_table))
                        impala.execute(refresh_table.format(stage_table))
                        impala.execute(create_query)
                    except Exception as e: # if impala doesn't work, switch to hive/spark
                        print(e)
                        print('trying with spark/hive...')
                        #is_impala = False
                        sqlContext.sql(create_query)
                else:
                    sqlContext.sql(create_query)
            elif is_reload: # this allows us to completely replace an existing table, based on the create_query
                print('dropping old version of table for reload...')
                if is_impala:
                    try:
                        impala.execute(drop_table.format(target_table))
                        impala.execute(invalidate_meta.format(stage_table))
                        impala.execute(refresh_table.format(stage_table))
                        impala.execute(create_query)
                    except Exception as e: # if impala doesn't work, switch to hive/spark
                        print(e)
                        print('trying with spark/hive...')
                        #is_impala = False
                        sqlContext.sql(drop_table.format(target_table))
                        sqlContext.sql(create_query)
                else:
                    sqlContext.sql(drop_table.format(target_table))
                    sqlContext.sql(create_query)
            else:
                print('inserting into table {}'.format(target_table))
                if is_impala:
                    try:
                        impala.execute(invalidate_meta.format(stage_table))
                        impala.execute(refresh_table.format(stage_table))
                        impala.execute(merge_query)
                    except Exception as e: # if impala doesn't work, switch to hive/spark
                        print(e)
                        print('trying with spark/hive...')
                        #is_impala = False
                        sqlContext.sql(merge_query)
                else:
                    sqlContext.sql(merge_query)
            
            if is_impala:
                impala.execute("refresh {}".format(target_table))
    except Exception as e:
        print(e)
    finally:
        sqlContext.sql("drop table if exists {} purge".format(stage_table))


# ## setup table kwargs, then run thru Pysqoop

# In[5]:


# tablename is preset in Setup Files. Overwrite if needed.
oracle_schema = "stage"
oracle_service = "DWBS01PU1"
schema = 'bdalab_sa'
hdfs_dir = '/data/CTL/encrypt/db/'
usr='sarabot'
my_tables = ["CDS_CONTACT_MEDIUM_PROFILE".lower(),"cds_customer_account"] # ,"cds_contact_medium_profile_ref"
for tablename in my_tables:
   oracle_query = "select * from {}.{}".format(oracle_schema,tablename)
   try:
       pysqoop(tablename=tablename, 
               oracle_schema=oracle_schema,
               oracle_service=oracle_service,
               schema=schema, 
               hdfs_dir=hdfs_dir,
               is_impala=True,
               is_reload=True,
               usr=usr,
               pwd=keyring.get_password(oracle_service, usr)
               )
   except Exception as e:
       print(e)


import json
import os
import psycopg2
import config


dir_path = os.path.dirname(os.path.realpath(__file__))
params = config.Config(dir_path +'/db.cfg')

# create database
con = psycopg2.connect(**params.as_dict()['bdg'])
con.autocommit = True
cur = con.cursor()
cur.execute("CREATE DATABASE test_bdg;")
cur.close()
con.close()

# create table
with psycopg2.connect(**params.as_dict()['bdg'], database='test_bdg') as con:
    con.cursor().execute(open(dir_path + '/create_customers_table.sql', 'r', encoding='utf-8').read())
    con.commit()

# add data
with psycopg2.connect(**params.as_dict()['bdg'], database='test_bdg') as con:
    con.cursor().execute(open(dir_path + '/customers_data_1x.sql', 'r', encoding='utf-8').read())
    con.commit()

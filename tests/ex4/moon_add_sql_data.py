import json
import os
import psycopg2
import config
import sys


dir_path = os.path.dirname(os.path.realpath(__file__))
params = config.Config(dir_path + '/db.cfg')
factor = 1
try:
    factor = sys.argv[1]
except IndexError:
    print('Please provide a factor number as an argument. Example: python3 moon_add_sql_data.py 1')

# create database
con = psycopg2.connect(**params['moon']['sql_db'])
con.autocommit = True
cur = con.cursor()
cur.execute("DROP DATABASE IF EXISTS index_bc;")
cur.execute("CREATE DATABASE index_bc;")
cur.execute("DROP DATABASE IF EXISTS test4;")
cur.execute("CREATE DATABASE test4;")
cur.close()
con.close()

# create index_bc table
with psycopg2.connect(**params['moon']['sql_db'], database='index_bc') as con:
    con.cursor().execute('CREATE TABLE sales_index(id varchar, bc_entry varchar);')
    con.commit()

# create table
with psycopg2.connect(**params['moon']['sql_db'], database='test4') as con:
    con.cursor().execute(open(dir_path +
                              '/create_customers_table.sql', 'r', encoding='utf-8').read())
    con.commit()

# add data
with psycopg2.connect(**params['moon']['sql_db'], database='test4') as con:
    con.cursor().execute(open(dir_path +
                              '/customers_data_' + str(factor) + 'x.sql', 'r', encoding='utf-8').read())
    con.commit()

import json
import os
import subprocess
from io import open
from datetime import datetime
import config
import psycopg2
from models import Model, Attribute
from strings import bcolors, progressBar


def update_bdg_catalog(params, catalog_file, db_name):
    """ Update BigDAWG catalog and schemas

    Args:
        params (_type_): _description_
        catalog_file (_type_): _description_
        db_name (_type_): _description_
    """

    # Add databases
    con = psycopg2.connect(**params.as_dict()['catalog'])
    cur = con.cursor()

    # add relational DB
    cur.execute(f"INSERT INTO catalog.databases VALUES (8, 1, '{db_name}', 'postgres', 'test');")

    i = 52
    for model in catalog_file['entities']:
        db = 7 if model['name'] == 'sales' else 8
        attribute_names = [attribute['name'] for attribute in model['attributes']]
        cur.execute(f"INSERT INTO catalog.objects VALUES ({i}, '{model['name']}', '{','.join(attribute_names)}',{db}, {db});")
        i += 1

    cur.close()
    con.commit()
    con.close()

    con = psycopg2.connect(**params.as_dict()['schemas'])
    cur = con.cursor()

    for model in catalog_file['entities']:
        attribute_name_and_types = [attribute['name'] + ' ' + attribute['type'] for attribute in model['attributes']]
        cur.execute(f"CREATE TABLE {model['name']} ({','.join(attribute_name_and_types)});")
        i += 1

    cur.close()
    con.commit()
    con.close()

if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    catalog_file = json.load(open(dir_path + '/../moon/mapper/catalog.json', 'r', encoding='utf-8'))
    params = config.Config(dir_path +'/db.cfg')

    update_bdg_catalog(params, catalog_file, 'test3')

#!/usr/bin/python

from __future__ import print_function
import os
import sys
import subprocess as sp
import argparse
import sqlite3 as sql

import ipdb

from scidbload import scidbload

SCIDB_VER = os.environ.get('SCIDB_VER', '13.12')
HOST = 'localhost'
BASE_PATH = '/media/scidb/scidb1312'

PATH = os.path.dirname(os.path.realpath(__file__))
ENGINE = os.path.join(PATH, 'scb_backup.db')

'''
Backup/Restore Functions
~~~~~~~~~~~~~~~~~~~~~~~~
'''

def backup(array_list=None, host=HOST):
    list_ = scidbload.List(host=host)
    if not array_list:
        array_list = list_.name_list    
    for array in array_list:
        try:
            assert array in list_.name_list
        except:
            print('Array {} not found ... skipping'.format(array))
            continue
        array_entry = save_opaque(array, host=host)
        insert_array(array_entry)

def restore(array_list, version='13.12', host=HOST):
    list_ = scidbload.List(host=host)
    for array in array_list:
        try:
            assert array not in list_.name_list
        except:
            print('Array {} already exists ... skipping'.format(array))
            continue
        array_entry = select_array(array)
        load_opaque(array_entry, host=host)

def save_opaque(array_name, host=HOST):
    sdb_array = scidbload.ScidbArray(array_name, host=HOST)
    description = sdb_array.description.anonymous_schema
    aql_ = ['SAVE', array_name, 'INTO CURRENT INSTANCE', 
            "'{}.opaque'".format(array_name), "AS 'OPAQUE'"]
    iquery(aql_, host=host, aql=True)
    return {'name': array_name, 'description': description}

def load_opaque(array_entry, host=HOST, scidb_ver=SCIDB_VER):
    '''NOTE: Array entry equals name and description. '''
    path = generate_path(array_entry['name'])
    array_entry.update({'path': path})
    aql_ = ['CREATE ARRAY {name} {description}'.format(**array_entry)]
    iquery(aql_, host=host, aql=True)
    aql_ = ['LOAD', array_entry['name'], 'FROM CURRENT INSTANCE', 
            "'{}'".format(path), "AS 'OPAQUE'"]
    #afl_ = "store(input({description}, -2, '{path}', 'OPAQUE'), {name})".format(**array_entry)
    iquery(aql_, host=host, aql=True) #  Always current version from os.environ


def generate_path(array_name, scidb_ver=SCIDB_VER):
    path_ = '000/0/{}.opaque'.format(array_name)
    return os.path.join(BASE_PATH, path_)

'''
SQL
~~~
'''

def reset_sql():
    if raw_input('Type "YES" to continue: ') == 'YES':
        with sql.connect(ENGINE) as db:
            try:
                cur = db.cursor()
                cur.execute('DROP TABLE Backup')
                db.commit()
            except Exception as e:
                print(e)
        with sql.connect(ENGINE) as db:
            cur.execute('CREATE TABLE Backup (name text primary key, description text)')
            db.commit()

def insert_array(array_entry):
    with sql.connect(ENGINE) as db:
        cur = db.cursor()
        cur.execute(('INSERT OR IGNORE INTO Backup (name, description) '
            'VALUES (:name, :description)'), array_entry)
        cur.execute(('UPDATE Backup SET name=:name, description=:description '
            'WHERE name=:name'), array_entry)
        db.commit()

def select_array(array_name):
    with sql.connect(ENGINE) as db:
        cur = db.cursor()
        cur.execute('SELECT * FROM Backup WHERE name = ?', (array_name, ))
        description = dict(zip(['name','description'], cur.fetchone()))
    return description

'''
SciDB
~~~~~
'''

def iquery(query, wait=True, host=HOST, aql=False):
    ''' execute scidb query using subprocess '''
    if not aql:
        lan = 'a'
    else:
        lan = ''
    query = ' '.join(query)
    bin_ = os.path.join('/opt', 'scidb', SCIDB_VER, 'bin', 'iquery')
    qU = [bin_, "-{0}nq {1}".format(lan, query)]
    print(' '.join(qU))
    sp1 = sp.Popen(qU, shell=False)
    if wait:
        sp1.communicate()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='''Python library for 
        backing up SciDB arrays.''')
    parser.add_argument('-I', '--init', help='Initialize DB-Store.',
            action='store_true', dest='initialize')
    parser.add_argument('-b', '--backup', help='Backup SciDB arrays.',
            action='store_true', dest='backup')
    parser.add_argument('-r', '--restore', help='Restore SciDB arrays.',
            action='store_true', dest='restore')
    parser.add_argument('-A', '--arrays', help='SciDB array list. ',
            default=None, type=str, nargs='+', dest='arrays')
    parser.add_argument('-H', '--host', help='SciDB host. ',
            default=None, type=str, dest='host')
    parser.add_argument('-v', '--version', help='SciDB version of restore.', 
            default=None, type=str, dest='version')

    args = parser.parse_args()
    
    if args.host:
        HOST = args.host
    if args.version:
        version = args.version
    else:
        version = SCIDB_VER
    if args.initialize:
        reset_sql()
    if args.restore:
        restore(args.arrays, version)
    if args.backup:
        backup(args.arrays)
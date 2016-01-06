#!/usr/bin/python

from __future__ import print_function
import os
from datetime import datetime
import argparse
import sqlite3 as sql

import scidbpy as sdb

# BACKUP_PATH is default backup save directory - change for your needs
BACKUP_PATH= '/data/kyle3/nick/scidb_backup'

PATH = os.path.dirname(os.path.realpath(__file__))
ENGINE = os.path.join(PATH, 'sdb_backup.sqlite3')


'''
SciDB Server URL
~~~~~~~~~~~~~~~~
'''


def set_host(host='127.0.0.1', port='8080'):
    """

    :param host: host ip.
    :param port: port of shim service.
    :return:
    """
    global HOST
    HOST = 'http://{host}:{port}'.format(**dict(host=host, port=port))
    return HOST


set_host()

'''
SciDB record list
~~~~~~~~~~~~~~~~~
'''


def get_array_record(host):
    """
    Get list of scidb arrays.

    :param host: url to shim.
    """

    with sdb.connect(host) as con:
        array_dict = con.list_arrays()
    return array_dict


'''
Backup/Restore test array
~~~~~~~~~~~~~~~~~~~~~~~~~
'''


def create_test_array(host):
    """
    Create test array in SciDB.

    :param host: url to shim.
    """
    with sdb.connect(host) as con:
        con.query("""
            store(
                redimension(
                    join(
                        build(<x:double>[k=0:8,1,0], k),
                    join(
                        build(<i:int64>[k=0:8,1,0], k%3),
                        build(<j:int64>[k=0:8,1,0], k/3))),
                    <x:double>[i=0:8,1,0, j=0:8,1,0]),
                scidbbackup_test_array)""")

def remove_test_array(host):
    """
    Remove the test array from SciDB.

    :param host: url to shim.
    """
    with sdb.connect(host) as con:
        con.remove('scidbbackup_test_array')


'''
Backup/Restore Functions
~~~~~~~~~~~~~~~~~~~~~~~~
'''


def backup(array_list, host, array_record=None):
    """
    Backup scidb array to backup file.

    :param array_list: list of scidb arrays for backup.
    :param host: url to shim.
    :param path: path to save backup binaries.
    :param array_record: current scidb arrays.
    """
    if array_record is None:
        array_record = get_array_record(host)
    for array_name in array_list:
        try:
            assert array_name in array_record
        except:
            print('Array {} not found ... skipping'.format(array_name))
            continue
        schema = get_schema(array_name, array_record)
        path = get_array_path(array_name)
        save_opaque(array_name, host, path)
        backup_record = BackupRecord(array_name=array_name,
                                     schema=schema,
                                     path=path)
        insert_backup_record(backup_record)

def get_schema(array_name, array_record):
    array_info_list = array_record[array_name]
    return array_info_list[1].lstrip(array_name)


def restore(array_list, host, array_record=None):
    """
    Restore scidb array from backup file.

    :param array_list: list of scidb arrays for restore.
    :param host: url to shim.
    :param path: path to save backup binaries.
    :param array_record: current scidb arrays.
    """
    if array_record is None:
        array_record = get_array_record(host)
    for array_name in array_list:
        try:
            assert array_name not in array_record
        except:
            print('Array {} already exists ... skipping'.format(array_name))
            continue
        array_backup_record = select_backup_record(array_name)
        load_opaque(array_backup_record['array_name'], array_backup_record['schema'],
                    array_backup_record['path'], host)


def save_opaque(array_name, host, path):
    """
    Save scidb array as opaque binary array.

    :param array_name: name of array to save
    :param host: url to shim.
    """
    with sdb.connect(host) as con:
        con.query("save({array_name}, '{path}', -2, 'OPAQUE')", array_name=array_name, path=path)


def load_opaque(array_name, schema, path, host):
    """
    Load scidb array from opaque binary array.
    :param array_name: name of array to save
    :param host: url to shim.
    """
    array_entry = select_backup_record(array_name)
    with sdb.connect(host) as con:
        con.query("create array {array_name} {schema}", array_name=array_name, schema=schema)
        con.query("load({array_name}, '{path}', -2, 'OPAQUE')", array_name=array_name, path=path)


def get_array_path(array_name):
    return os.path.join(BACKUP_PATH, array_name + '.opaque')

'''
Backup Recordkeeping (SQL)
~~~~~~~~~~~~~~~~~~~~~~~~~~
'''


class BackupRecord(dict):
    def __init__(self, array_name=None, schema=None, path=None):
        """
        Record class.

        :param array_name: name of scidb array
        :param schema: schema of scidb array
        :param path: path to backup-file
        """
        self['array_name'] = array_name
        self['schema'] = schema
        self['datetime'] = datetime.now()
        self['path'] = path
        try: # for empty backup-records or debugging
            self['size'] = os.path.getsize(path)
        except:
            self['size'] = 0

    def __call__(self, array_name, schema, path, datetime, size):
        self['array_name'] = array_name
        self['schema'] = schema
        self['path'] = path
        self['datetime'] = datetime
        self['size'] = size
        return self

def reset_sql():
    """
    Inits sqlite table
    """
    if raw_input('WARNING/DANGER: Resetting backup records -- type "delete" to continue: ') == 'delete':
        with sql.connect(ENGINE) as db:
            try:
                cur = db.cursor()
                cur.execute('DROP TABLE Backup')
                db.commit()
            except Exception as e:
                print(e)
        init_table()


def init_table():
    with sql.connect(ENGINE) as db:
        cur = db.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS Backup
                          (array_name TEXT PRIMARY KEY,
                           schema TEXT,
                           path TEXT,
                           datetime DATETIME,
                           size INTEGER)''')
        db.commit()


def insert_backup_record(backup_record):
    """
    Insert backup record.

    :type backup_record: BackupRecord
    :param backup_record: backup array information.
    """
    with sql.connect(ENGINE) as db:
        cur = db.cursor()
        cur.execute("""
                    INSERT OR IGNORE INTO Backup (array_name, schema , path, datetime, size)
                    VALUES (:array_name, :schema, :datetime, :path, :size)""", backup_record)
        cur.execute("""
                     UPDATE Backup SET array_name=:array_name, schema=:schema, path=:path, datetime=:datetime, size=:size
                     WHERE array_name=:array_name""", backup_record)
        db.commit()


def select_backup_record(array_name):
    """
    Select backup record by name.

    :param array_name: name of scidb array
    :return: backup_record
    """
    with sql.connect(ENGINE) as db:
        cur = db.cursor()
        cur.execute('SELECT * FROM Backup WHERE array_name = ?', (array_name,))
        description = cur.fetchone()
    return BackupRecord()(*description)


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
    parser.add_argument('-p', '--path', help='Path to backup files.',
                        default=BACKUP_PATH, dest='path', type=str)
    parser.add_argument('-H', '--host', help='SciDB host name for the cluster instance. ',
                        type=str, dest='host', default='127.0.0.1')
    parser.add_argument('-P', '--port', help='Port for connection. ',
                        type=str, dest='port', default='8080')
    parser.add_argument('-v', '--version', help='SciDB version of restore.',
                        default=os.environ.get('SCIDB_VER', '15.7'), type=str, dest='version')

    args = parser.parse_args()

    HOST = 'http://{host}:{port}'.format(**args)
    BACKUP_PATH = args.backup

    if args.initialize:
        reset_sql()
    if args.restore:
        restore(args.arrays, args.version)
    if args.backup:
        backup(args.arrays)

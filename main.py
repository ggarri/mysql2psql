__author__ = 'ggarrido'

import json
import sys
import os
import time

from libs.MysqlParser import MysqlParser
from libs.RuleHandler import RuleHandler
from libs.PsqlParser import PsqlParser
from multiprocessing import Pool
import multiprocessing
import subprocess
import traceback

try: import psycopg2
except ImportError: import psycopg2cffi as psycopg2


path = os.path.dirname(os.path.realpath(__file__))
MAX_THREADS, skip_pre_sql, pending_dbs, terminate = multiprocessing.cpu_count(), False, None, False
information_schema = 'information_schema'

def migrate(config, psql_conn_params, database, tables, skip_pre_sql, log_file=None):
    """
    Main executor, generate psql no-data dump file base on indicated db/tables from mysql connection
    :param config: Mysql db connection params and others
    :param database: Database to migrate
    :param tables: Table to migrate
    :return:
    """
    db_name = database
    output_path = os.path.join(path, 'output', db_name)
    tables_path = os.path.join(output_path, 'tables')

    # can trigger a race condition
    if not os.path.exists(output_path): os.mkdir(output_path, 0755)
    if not os.path.exists(tables_path): os.mkdir(tables_path, 0755)

    pg_conn = psycopg2.connect(**psql_conn_params)
    pg_cursor = pg_conn.cursor()

    mysql_conn_params = config['mysql']
    mysql_conn_params['db'] = db_name
    mysql_parser = MysqlParser(mysql_conn_params, information_schema)
    mysql_parser.set_skip_pre_sql(skip_pre_sql)
    mysql_schema = mysql_parser.get_schema(db_name, tables)

    # Generate psql schema, parsing psql rules(types, defaults...)
    # Write result into output/psql_schema.json
    psql_parser = PsqlParser(pg_cursor, pg_conn)

    try:
        with open(os.path.join(output_path, 'mysql_schema.json'), 'w') as outfile:
            json.dump(mysql_schema, outfile, indent=4, sort_keys=True)

        # Applying model rules (renaming, new defaults, ....)
        # Write result into output/mysql_schema_v2.json
        schema_changes = json.loads(open('./rules/schema_changes.json').read())
        rule_handler = RuleHandler(schema_changes)
        mysql_schema_v2 = rule_handler.obtain_modified_schema(mysql_schema)
        with open(os.path.join(output_path, 'mysql_schema_v2.json'), 'w') as outfile:
            json.dump(mysql_schema_v2, outfile, indent=4, sort_keys=True)
            outfile.close()

        psql_schema = psql_parser.get_schema_from_mysql(mysql_schema_v2)
        with open(os.path.join(output_path, 'psql_schema.json'), 'w') as outfile:
            json.dump(psql_schema, outfile, indent=4, sort_keys=True)
            outfile.close()

        # Generate psql create table queries from psql schema generated on previous step
        # Write result into output/psql_tables.sql
        timeS, msg = time.time(), "Generating Schema...   "
        print msg
        if log_file: log_file.write(msg)
        psql_parser.generate_sql_schema(psql_schema, 'public', os.path.join(output_path, 'psql_tables.sql'))
        print time.time() - timeS
        if log_file: log_file.write(str(time.time() - timeS)+'\n')


        # Generate mysql dump file
        timeS, msg = time.time(), "Generating raw data...(might take few minutes)"
        print msg
        if log_file: log_file.write(msg)
        psql_parser.generate_dump_from_raw(mysql_parser, db_name, psql_schema, 'public',
                                               os.path.join(output_path, 'psql_data.sql'), tables_path, schema_changes)

        print time.time() - timeS
        if log_file: log_file.write(str(time.time() - timeS)+'\n')


        timeS, msg = time.time(), "Generating indexes and fk...   "
        print msg
        if log_file: log_file.write(msg)
        psql_parser.generate_psql_index_fk(mysql_schema_v2, os.path.join(output_path, 'psql_index_fk.sql'))
        print time.time() - timeS
        if log_file: log_file.write(str(time.time() - timeS)+'\n')


        # Generate vies in case it is a client db
        if 'v1_schema_name' in config and config['v1_schema_name'] and len(config['v1_schema_name']) > 0:
            timeS, msg = time.time(), "Generating views...   "
            print msg
            if log_file: log_file.write(msg)
            psql_parser.generate_psql_views(mysql_schema_v2, config['v1_schema_name'], 'public',
                                                        os.path.join(output_path, 'psql_views.sql'))
        else:
            open(os.path.join(output_path, 'psql_views.sql'), 'w').close()
        print time.time() - timeS
        if log_file: log_file.write(str(time.time() - timeS)+'\n')
    # except:
    #     e = sys.exc_info()[0]
    #     print "ERROR: %s" % str(e)
    finally:
        mysql_parser.close()
        pg_cursor.close()

def get_all_databases(config):
    mysql_conn_params = config['mysql']
    mysql_parser = MysqlParser(mysql_conn_params, information_schema)
    dbs = mysql_parser.get_all_databases(config['prefix'])
    mysql_parser.close()
    return dbs

def migrate_db(params, psql_conn_params, database, tables=[], skip_pre_sql=False):
    print '-------------------------------------'
    print '\t %s ' % (database)
    print '-------------------------------------'

    try:
        log_file_path = os.path.join(path, 'logs', database+'.log')
        log_file = open(log_file_path, 'w', 0)
        migrate(config, psql_conn_params, database, tables, skip_pre_sql, log_file)
        timeS = time.time()
        print "Running ./bin/migrate.sh .....logs in " + log_file_path
        subprocess.check_call(['bash', path+'/bin/migrate.sh', '-h', config['psql']['host'], '-d', database, '-Wf', config['psql']['password'], '-p',
                               str(config['psql']['port']), '-U', config['psql']['user']]
                              , stderr=log_file, stdout=log_file)
        print time.time() - timeS
    except  Exception:
        e = sys.exc_info()[0]
        print "ERROR: %s" % str(e)
        log_file.write("Python exception during generating\n")
        log_file.write("ERROR: %s" % e)
        print traceback.format_exc()
    finally:
        log_file.close()
        return database

def migration_completed(database):
    global pending_dbs; pending_dbs -= 1
    log_file_path = os.path.join(path, 'logs', database+'.log')
    log_file = open(log_file_path, 'r')
    print log_file.read()
    log_file.close()
    return database

def test_f(params, pg_cursor, database, tables=[]):
    print params, pg_cursor, database, tables
    return database

if __name__ == '__main__':
    database = sys.argv[1]
    tables = sys.argv[2:]

    config = json.loads(open('./config/parameters.json').read())
    databases = [database] if database != "all-databases" else get_all_databases(config)
    pending_dbs = len(databases)
    isThreading, n_threads = False, 0
    if pending_dbs > 1 and 'threads' in config and int(config['threads']) > 0:
        isThreading = True
        if int(config['threads']) > MAX_THREADS: print "WARNING: Max number of threads are %d" % config['threads']
        n_threads =  MAX_THREADS if (int(config['threads']) > MAX_THREADS) else config['threads']

    psql_conn_params = config['psql']
    psql_conn_params['dbname'] = 'postgres'
    if isThreading: pool = Pool(processes=n_threads)

    for database in databases:
        if isThreading: pool.apply_async(migrate_db, [config, psql_conn_params, database, tables, skip_pre_sql], callback=migration_completed)
        else: migrate_db(config, psql_conn_params, database, tables, skip_pre_sql); migration_completed(database)
    try:
        while isThreading and pending_dbs>0: print "Pending dbs %s..." % pending_dbs; sys.stdout.flush(); time.sleep(5)
    except KeyboardInterrupt: terminate=True; print "Interrupt!!!"

    if isThreading:
        if terminate: pool.terminate()
        else:
            print "Waiting threads to complete"; pool.close()
            print "Waiting threads to wrap-up"; pool.join()

#!/usr/bin/env python
''' 

This script creates the tables and schema for a RDS postgres instance

This thing loads up from a dump file, ONLY run it when creating a new 
db instance, otherwise you will overwrite all existing data...

You must create a ~/.pgpass file with the database password for this to 
work, see the Neon Wiki for more information on this 

''' 
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from contextlib import closing
import psycopg2
from subprocess import call
import utils
import utils.neon
from utils.options import define, options

define("pg_db_name", default="cmsdb", type=str, help="PG Database Name")
define("pg_db_user", default="pgadmin", type=str, help="PG Database User")
define("pg_db_host", default=None, type=str, help="PG Database Hostname")
define("pg_db_port", default=5432, type=int, help="PG Database Port")

def main():
    dump_file = '%s/cmsdb/migrations/cmsdb.sql' % (__base_path__)
    cmd = '/usr/bin/psql --quiet -p %d -h %s --username=%s %s < %s' % (options.get('pg_db_port'), 
                                                                       options.get('pg_db_host'), 
                                                                       options.get('pg_db_user'), 
                                                                       options.get('pg_db_name'),
                                                                       os.path.join(os.getcwd(), dump_file))
    call(cmd, shell=True)

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

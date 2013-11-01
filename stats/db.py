'''Helper functions to deal with the stats database.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import inspect
import logging
import re
import string
import sys

from utils.options import define, options

_log = logging.getLogger(__name__)

def create_tables(cursor):
    '''Creates all the tables needed in the stats database if they don't exist.
    '''
    cursor.execute('''CREATE TABLE IF NOT EXISTS hourly_events (
                   thumbnail_id VARCHAR(32) NOT NULL,
                   hour DATETIME NOT NULL,
                   loads INT NOT NULL DEFAULT 0,
                   clicks INT NOT NULL DEFAULT 0,
                   UNIQUE (thumbnail_id, hour))''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS last_update (
                      tablename VARCHAR(256) NOT NULL UNIQUE,
                      logtime DATETIME)''')

def execute(cursor, command, args=[]):
    '''Executes a command on a sql cursor, but handles parameters properly.

    The DBAPI doesn't specify a standard parameter substituion. Some
    db's like '?' parameter substitution, while others like %s
    substitution. To avoid this problem, call this function instead of
    cursor.execute wiht %s parameter substitution.

    So, now instead of doing:
    cursor.execute('SELECT * from t1 where c=?', ('hi',))

    You should:
    execute(cursor, 'SELECT * from t1 where c=%s', ('hi',))

    '''
    mod_name = inspect.getmodule(cursor.__class__).__name__
    root_mod = re.compile('^([a-zA-Z0-9_]+)').search(mod_name).group(1)
    pstyle = sys.modules[root_mod].paramstyle
    
    if pstyle == 'qmark':
        command = string.replace(command, '%s', '?')

    elif pstyle == 'format':
        pass 

    else:
        raise NotImplementedError('This DB backend type is not supported.')

    return cursor.execute(command, args)

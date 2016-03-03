# -*- coding: utf-8 -*-
#  Copyright 2013 Takeshi KOMIYA
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import signal
import psycopg2
import tempfile
import subprocess
from cmsdb import neondata
from glob import glob
from time import sleep
from shutil import copytree, rmtree
from datetime import datetime
from contextlib import closing
from . import net
from utils.options import define, options
from subprocess import call

__all__ = ['Postgresql', 'skipIfNotFound']

SEARCH_PATHS = (['/usr/local/pgsql', '/usr/local'] +
                glob('/usr/lib/postgresql/9.4') +  # for Debian/Ubuntu
                glob('/usr/lib/postgresql/9.5') +  # for Debian/Ubuntu
                glob('/opt/local/lib/postgresql*'))  # for MacPorts
DEFAULT_SETTINGS = dict(auto_start=2,
                        base_dir=None,
                        initdb=None,
                        initdb_args='-U postgres -A trust',
                        postgres=None,
                        postgres_args='-h 127.0.0.1 -F -c logging_collector=off',
                        pid=None,
                        port=None,
                        dump_file=None, 
                        dbname='test')

class Postgresql(object):
    def __init__(self, **kwargs):
        self.settings = dict(DEFAULT_SETTINGS)
        self.settings.update(kwargs)
        self.pid = None
        self._owner_pid = os.getpid()
        #self.old_port = None
        #self.old_name = None 
        self._use_tmpdir = False

        if self.base_dir:
            if self.base_dir[0] != '/':
                self.settings['base_dir'] = os.path.join(os.getcwd(), self.base_dir)
        else:
            self.settings['base_dir'] = tempfile.mkdtemp()
            self._use_tmpdir = True

        if self.initdb is None:
            self.settings['initdb'] = find_program('initdb', ['bin'])

        if self.postgres is None:
            self.settings['postgres'] = find_program('postgres', ['bin'])

        if self.auto_start:
            if self.auto_start >= 2:
                self.setup()

            self.start()

    def __del__(self):
        self.stop()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()

    def __getattr__(self, name):
        if name in self.settings:
            return self.settings[name]
        else:
            raise AttributeError("'Postgresql' object has no attribute '%s'" % name)

    def dsn(self, **kwargs):
        # "database=test host=localhost user=postgres"
        params = dict(kwargs)
        params.setdefault('port', self.port)
        #params.setdefault('host', '127.0.0.1')
        params.setdefault('host', '127.0.0.1')
        params.setdefault('user', 'postgres')
        params.setdefault('database', self.dbname)
        #import pdb; pdb.set_trace()

        return params

    def url(self, **kwargs):
        params = self.dsn(**kwargs)

        url = ('postgresql://%s@%s:%d/%s' %
               (params['user'], params['host'], params['port'], params['database']))

        return url

    def setup(self):
        # (re)create directory structure
        for subdir in ['data', 'tmp']:
            path = os.path.join(self.base_dir, subdir)
            if not os.path.exists(path):
                os.makedirs(path)
                os.chmod(path, 0o700)

        # initdb
        if not os.path.exists(os.path.join(self.base_dir, 'data', 'PG_VERSION')):
            args = ([self.initdb, '-D', os.path.join(self.base_dir, 'data'), '--lc-messages=C'] +
                    self.initdb_args.split())

            try:
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, err = p.communicate()
                if p.returncode != 0:
                    raise RuntimeError("initdb failed: %r" % err)
            except OSError as exc:
                raise RuntimeError("failed to spawn initdb: %s" % exc)

    def start(self):
        if self.pid:
            return  # already started

        if self.port is None:
            self.port = net.find_free_port()

        logger = open(os.path.join(self.base_dir, 'tmp', 'postgresql.log'), 'wt')
        pid = os.fork()
        if pid == 0:
            os.dup2(logger.fileno(), sys.__stdout__.fileno())
            os.dup2(logger.fileno(), sys.__stderr__.fileno())

            try:
                os.execl(self.postgres, self.postgres,
                         '-p', str(self.port),
                         '-D', os.path.join(self.base_dir, 'data'),
                         '-k', os.path.join(self.base_dir, 'tmp'),
                         *self.postgres_args.split())
            except Exception as exc:
                raise RuntimeError('failed to launch postgres: %r' % exc)
        else:
            logger.close()

            self.pid = pid
            exec_at = datetime.now()
            sleep(0.1)
            while True:
                if os.waitpid(pid, os.WNOHANG)[0] != 0:
                    raise RuntimeError("*** failed to launch postgres ***\n" + self.read_log())

                if self.is_connection_available():
                    break

                if (datetime.now() - exec_at).seconds > 10.0:
                    raise RuntimeError("*** failed to launch postgres (timeout) ***\n" + self.read_log())

                sleep(0.1)

            # create test database
            with closing(psycopg2.connect(**self.dsn(database='postgres'))) as conn:
                conn.autocommit = True
                with closing(conn.cursor()) as cursor:
                    cursor.execute("SELECT COUNT(*) FROM pg_database WHERE datname='%s'" % self.dbname)
                    if cursor.fetchone()[0] <= 0:
                        cursor.execute('CREATE DATABASE %s' % self.dbname)
                    if self.dump_file:
                        cmd = '/usr/bin/psql --quiet -p %d -h 127.0.0.1 --username=postgres %s < %s' % (self.port, self.dbname, os.path.join(os.getcwd(), self.dump_file))
                        call(cmd, shell=True)

        self.old_port = options.get('cmsdb.neondata.db_port')
        self.old_name = options.get('cmsdb.neondata.db_name')
        options._set('cmsdb.neondata.db_port', self.port)
        options._set('cmsdb.neondata.db_name', self.dbname)
    
    def clear_all_tables(self): 
        with closing(psycopg2.connect(**self.dsn(database='postgres', dbname=self.dbname))) as conn:
            conn.autocommit = True
            with closing(conn.cursor()) as cursor:
                cursor.execute("DELETE FROM abstractintegration")
                cursor.execute("DELETE FROM abstractplatform")
                cursor.execute("DELETE FROM cdnhostingmetadatalist")
                cursor.execute("DELETE FROM experimentstrategy")
                cursor.execute("DELETE FROM neonapikey")
                cursor.execute("DELETE FROM neonapirequest")
                cursor.execute("DELETE FROM neonuseraccount")
                cursor.execute("DELETE FROM neonplatform")
                cursor.execute("DELETE FROM users")
                cursor.execute("DELETE FROM request")
                cursor.execute("DELETE FROM thumbnailmetadata")
                cursor.execute("DELETE FROM thumbnailservingurls")
                cursor.execute("DELETE FROM thumbnailstatus")
                cursor.execute("DELETE FROM videometadata")
                cursor.execute("DELETE FROM videostatus")
        neondata.PostgresPubSub.instance = None  

    def stop(self, _signal=signal.SIGINT):
        options._set('cmsdb.neondata.db_port', self.old_port)
        options._set('cmsdb.neondata.db_name', self.old_name)
        neondata.PostgresDB.instance = None  
        neondata.PostgresPubSub.instance = None  
        self.terminate(_signal)
        self.cleanup()

    def terminate(self, _signal=signal.SIGINT):
        if self.pid is None:
            return  # not started

        if self._owner_pid != os.getpid():
            return  # could not stop in child process

        try:
            os.kill(self.pid, _signal)
            killed_at = datetime.now()
            while (os.waitpid(self.pid, os.WNOHANG)):
                if (datetime.now() - killed_at).seconds > 10.0:
                    os.kill(self.pid, signal.SIGKILL)
                    raise RuntimeError("*** failed to shutdown postgres (timeout) ***\n" + self.read_log())

                sleep(0.1)
        except OSError:
            pass

        self.pid = None

    def cleanup(self):
        if self.pid is not None:
            return
      
        if self._use_tmpdir and os.path.exists(self.base_dir):
            rmtree(self.base_dir, ignore_errors=True)
            self._use_tmpdir = False

    def read_log(self):
        try:
            with open(os.path.join(self.base_dir, 'tmp', 'postgresql.log')) as log:
                return log.read()
        except Exception as exc:
            raise RuntimeError("failed to open file:tmp/postgresql.log: %r" % exc)

    def is_connection_available(self):
        try:
            params = self.dsn(database='template1')
            with closing(psycopg2.connect(**params)):
                pass
        except psycopg2.OperationalError:
            return False
        else:
            return True


def skipIfNotInstalled(arg=None):
    if sys.version_info < (2, 7):
        from unittest2 import skipIf
    else:
        from unittest import skipIf

    def decorator(fn, path=arg):
        if path:
            cond = not os.path.exists(path)
        else:
            try:
                find_program('postgres', ['bin'])  # raise exception if not found
                cond = False
            except:
                cond = True  # not found

        return skipIf(cond, "PostgreSQL not found")(fn)

    if callable(arg):  # execute as simple decorator
        return decorator(arg, None)
    else:  # execute with path argument
        return decorator

skipIfNotFound = skipIfNotInstalled

def find_program(name, subdirs):
    path = get_path_of(name)
    if path:
        return path

    for base_dir in SEARCH_PATHS:
        for subdir in subdirs:
            path = os.path.join(base_dir, subdir, name)
            if os.path.exists(path):
                return path

    raise RuntimeError("command not found: %s" % name)

def get_path_of(name):
    path = subprocess.Popen(['/usr/bin/which', name],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).communicate()[0]
    if path:
        return path.rstrip().decode('utf-8')
    else:
        return None

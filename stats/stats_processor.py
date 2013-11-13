#!/usr/bin/env python
'''Runs all the necessary map reduce jobs to get statistics.


Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import atexit
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
import fnmatch
import logging
import os
import re
from stats.hourly_event_stats_mr import HourlyEventStats
import shutil
import signal
import tarfile
import tempfile
import time
import utils.ps

from utils.options import define, options

define('mr_conf', help='Config file for the map reduce jobs',
       default='./mrjob.conf')
define('input', default=None,
       help=('Glob specifying the input log files. '
             'Can be an s3 glob of the form s3://bucket/file* or '
             'a local glob'))
define('runner', default='local',
       help='Where to run the MapReduce. "emr", "local" or "hadoop"')
define('run_period', default=10800, type=int,
       help='Time in seconds between when the job should be run.')
define('min_new_files', default=1, type=int,
       help='Minimum number of new files in the input in order to run the job.')

# Options for HourlyEventStats
define('stats_host', default='stats.cnvazyzlgq2v.us-east-1.rds.amazonaws.com',
       help='Host of the stats database')
define('stats_port', type=int, default=3306,
       help='Port to the stats database')
define('stats_user', default='mrwriter', help='User for the stats database')
define('stats_pass', default='kjge8924qm',
       help='Password for the stats database')
define('stats_db', default='stats_dev', help='Stats database to connect to')
define('increment_stats', type=int, default=0,
       help='If true, stats are incremented. Otherwise, they are overwritten')
define('stats_table', default='hourly_events',
       help='Table in the stats database to write to')

_log = logging.getLogger(__name__)

def tar_src_tree():
    '''Tars up the source tree so that it can be sent to the machines
    running the job.
    
    Returns:
    The name of the archive
    '''
    tarstream = tempfile.NamedTemporaryFile(delete=False, suffix='.tar.gz')
    source_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    with tarfile.open(fileobj=tarstream, mode='w:gz',
                      format=tarfile.GNU_FORMAT) as archive:
        archive.add(os.path.join(source_root, 'utils'), 'utils')
        archive.add(os.path.join(source_root, 'stats'), 'stats')
        archive.add(os.path.join(source_root, 'api'), 'api')
        archive.add(os.path.join(source_root, 'supportServices'),
                    'supportServices')
        archive.add(os.path.join(source_root, 'NEON_ROOT'), 'NEON_ROOT')
        
    return tarstream.name

def must_download():
    '''Returns true if we need to download from S3.'''
    return options.runner == 'local' and options.input.startswith('s3://')

class DataDirectory:
    def __init__(self):
        self.localdir = tempfile.mkdtemp()

        # Figure out the path that the runner will need.
        if must_download():
            self.path = '%s/*' % self.localdir
        else:
            self.path = options.input

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        if self.localdir is not None:
            shutil.rmtree(self.localdir)
        self.localdir = None

    def erase(self):
        '''Erases all the files in the directory, but keeps the path the same.'''
        for path in os.listdir(self.localdir):
            os.remove(path)

    def count_files(self, runner):
        '''Count the number of files available for input.'''
        if must_download():
            self._sync_local_dir()

        return len([x for x in runner.fs.ls(self.path)])

    def _sync_local_dir(self):
        '''Synchronizes the local directory with S3.'''
        s3conn = S3Connection()

        s3pathRe = re.compile('s3://([0-9a-zA-Z_\-]+)/([0-9a-zA-Z_/\-\*]*)')
        bucket_name, key_match = s3pathRe.match(options.input).groups()

        bucket = s3conn.get_bucket(bucket_name)
        try:
            for key in BucketListResultSet(bucket):
                if fnmatch.fnmatch(key.name, key_match):
                    local_fn = os.path.join(self.localdir, key.name)
                    if not os.path.exists(local_fn):
                        key.get_contents_to_filename(local_fn)
        except S3ResponseError as e:
            if e.status == '404':
                _log.warn('Could not find key %s in bucket %s' %
                            (key.name, bucket_name))
            else:
                raise

    

def main(erase_local_data=None):
    '''The main routine.

    erase_local_data - An optional mutiprocessing.Event() that when
    set, will cause the local data to be erased.
    
    '''
    atexit.register(utils.ps.shutdown_children)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    os.environ['MRJOB_CONF'] = options.mr_conf

    archive_name = tar_src_tree()
    try:
        with DataDirectory() as data_dir:
            job = HourlyEventStats(args=[
                '-r', options.runner,
                '--python-archive', archive_name,
                '--stats_host', options.stats_host,
                '--stats_port', str(options.stats_port),
                '--stats_user', options.stats_user,
                '--stats_pass', options.stats_pass,
                '--stats_db', options.stats_db,
                '--stats_table', options.stats_table,
                '--increment_stats', str(options.increment_stats),
                '--neon_config', options.get_config_file(),
                data_dir.path])

            known_input_files = 0
            while True:
                if erase_local_data is not None and erase_local_data.is_set():
                    data_dir.erase()
                    erase_local_data.clear()
                
                _log.debug('Looking for new log files to process from %s' % 
                           options.input)
                try:
                    with job.make_runner() as runner:
                        n_files = data_dir.count_files(runner)
                        if (n_files - known_input_files) >= options.min_new_files:
                            _log.warn('Running stats processing job')
                            runner.run()
                            known_input_files = n_files
                            runner.print_counters()
                except Exception as e:
                    _log.exception('Unhandled error when processing stats: %s'
                                   % e)

                time.sleep(options.run_period)
    finally:
        os.remove(archive_name)

if __name__ == '__main__':
    utils.neon.InitNeon()
    main()

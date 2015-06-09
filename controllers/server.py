import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import datetime
import logging
import boto
import socket
import gzip
import json
import signal
import atexit
import multiprocessing
import tornado.gen
import tornado.ioloop
import utils.neon
import utils.ps
from utils import statemon
from cmsdb import neondata
from StringIO import StringIO
from utils.options import define, options
from boto.s3.connection import S3Connection
_log = logging.getLogger(__name__)

define("poll_period", default=300, help="Period(s) to poll",
       type=int)
define('s3_bucket', default='neon-image-serving-directives-test',
       help='Bucket to publish the serving directives to')
define('directive_filename', default='mastermind',
       help='Filename in the S3 bucket that will hold the directive.')

# Monitoring variables
statemon.define('cycles_complete', int)
statemon.define('unexpected_exception', int)
statemon.define('s3_connection_error', int)
statemon.define('update_experiment_error', int)
statemon.define('cycle_runtime', float)


class S3DirectiveWatcher(object):
    def __init__(self):
        self.last_update_time = datetime.datetime.utcnow()
        self.S3Connection = S3Connection

    def parse_directive_file(self, file_data):
        '''{(account_id, video_id) -> json_directive}'''
        gz = gzip.GzipFile(fileobj=StringIO(file_data), mode='rb')
        lines = gz.read().split('\n')

        directives = {}
        for line in lines[1:]:
            if len(line.strip()) == 0:
                # It's an empty line
                continue
            if line == 'end':
                break
            data = json.loads(line)
            if data['type'] == 'dir':
                directives[data['vid']] = data

        return directives

    def compare_dates(self, date1, date2):
        if self.datetime_from_modified(date1) >= date2:
            return True
        return False

    def datetime_from_modified(self, date_str):
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        return datetime.datetime.strptime(date_str, date_format)

    def get_bucket(self, bucket_name):
        # Create the connection to S3
        s3conn = self.S3Connection()

        try:
            bucket = s3conn.get_bucket(bucket_name)
        except boto.exception.BotoServerError as e:
            _log.error('Could not get bucket %s: %s' %
                       (bucket_name, e))
            statemon.state.increment('s3_connection_error')
            return None
        except boto.exception.BotoClientError as e:
            _log.error('Could not get bucket %s: %s' %
                       (bucket_name, e))
            statemon.state.increment('s3_connection_error')
            return None
        except socket.error as e:
            _log.error('Error connecting to S3: %s' % e)
            statemon.state.increment('s3_connection_error')
            return None
        return bucket

    def filter_and_get_files_from_s3(self, bucket, last_update_time):
        # getting within hour of last updated
        now = datetime.datetime.utcnow()
        bucket_listing = bucket.list(
            prefix=last_update_time.strftime('%Y%m%d%H'))

        # getting the current hour
        if (last_update_time.hour < now.hour):
            bucket_listing_now = bucket.list(prefix=now.strftime('%Y%m%d%H'))
            for key in bucket_listing_now:
                bucket_listing.append(key)

        # filter file by last modified
        filename_list = [
            i for i in bucket_listing
            if self.compare_dates(i.last_modified, last_update_time)
        ]

        return filename_list

    @tornado.gen.coroutine
    def read_directives(self, bucket, result_set):
        # parse the file to json
        file_content = bucket.get_key(result_set.name).get_contents_as_string()
        parsed = self.parse_directive_file(file_content)

        for key, value in parsed.iteritems():
            #  get video metada
            ecmd = yield tornado.gen.Task(
                neondata.ExperimentControllerMetaData.get,
                value['aid'], value['vid'].split('_', 1)[1])
            if ecmd:
                api_key = ecmd.get_api_key()
                # for each experiment - update directives
                for i in ecmd.controllers:
                    state = \
                        neondata.ControllerExperimentState.COMPLETE
                    if i['state'] == state:
                        continue
                    last_modified_epoch = (
                        self.datetime_from_modified(result_set.last_modified) -
                        datetime.datetime(1970, 1, 1)).total_seconds()

                    if last_modified_epoch <= i['last_process_date']:
                        continue

                    ctr = neondata.Controller.get(
                        i['controller_type'], api_key, i['platform_id'])

                    try:
                        state = yield ctr.update_experiment_with_directives(
                            i, value)

                        # update controller - last process date
                        ecmd.update_controller(
                            i['controller_type'], i['platform_id'],
                            i['experiment_id'], i['video_id'],
                            state, last_modified_epoch, i['extras'])
                        yield tornado.gen.Task(ecmd.save)

                    except ValueError as e:
                        _log.error("key=read_directives"
                                   " msg=controller api call failed")
                        _log.error(e.message)
                        statemon.state.increment('update_experiment_error')
        return

    @tornado.gen.coroutine
    def run_one_cycle(self):
        bucket = self.get_bucket(options.s3_bucket)
        if bucket is None:
            return

        bucket_files = self.filter_and_get_files_from_s3(
            bucket, self.last_update_time)

        yield [
            self.read_directives(bucket, x) for x in bucket_files
            if x is not None
        ]
        self.last_update_time = datetime.datetime.utcnow()


@tornado.gen.coroutine
def main(run_flag, directive_watcher):
    while run_flag.is_set():
        start_time = datetime.datetime.now()
        try:
            yield directive_watcher.run_one_cycle()
            statemon.state.increment('cycles_complete')
        except Exception:
            _log.exception('Uncaught exception when reading from Controller')
            statemon.state.increment('unexpected_exception')
        cycle_runtime = (datetime.datetime.now() - start_time).total_seconds()
        statemon.state.cycle_runtime = cycle_runtime

        if cycle_runtime < options.poll_period:
            yield tornado.gen.sleep(options.poll_period - cycle_runtime)
    _log.info('Finished program')

if __name__ == "__main__":
    utils.neon.InitNeon()

    directive_watcher = S3DirectiveWatcher()
    run_flag = multiprocessing.Event()
    run_flag.set()

    atexit.register(utils.ps.shutdown_children)
    atexit.register(run_flag.clear)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))

    tornado.ioloop.IOLoop.current().run_sync(
        lambda: main(run_flag, directive_watcher))

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import logging
import datetime
import time
import utils.neon
import utils.ps
import signal
import atexit
import threading
import multiprocessing
import tornado.gen
import tornado.ioloop
import boto
import socket
from StringIO import StringIO
import gzip
import json
from boto.s3.connection import S3Connection
from utils import statemon
from utils.options import define, options
from cmsdb import neondata
from controllers import neon_controller
_log = logging.getLogger(__name__)

define('s3_bucket', default='neon-image-serving-directives-test',
       help='Bucket to publish the serving directives to')
define('directive_filename', default='mastermind',
       help='Filename in the S3 bucket that will hold the directive.')
define("interval", default=60, type=int,
       help="Standby time (in seconds) to execute loop again. \
            Must be between 60 and 1200.")
define("start_time", default="time_year_...", type=str,
       help="curtime.strftime('%Y%m%d%H%M%S')")

# Monitoring variables
statemon.define('reading_error', int)  # error reading directive to s3
statemon.define('last_directive_watcher_update_time', float)  #
statemon.define('time_since_last_read', float)  # Time since last read


class S3DirectiveWatcher(threading.Thread):
    def __init__(self, activity_watcher=utils.ps.ActivityWatcher()):
        super(S3DirectiveWatcher, self).__init__(name='S3DirectiveWatcher')

        self.activity_watcher = activity_watcher
        self.last_update_time = datetime.datetime.utcnow()
        self._update_timer = None
        self._update_time_last_reading()

        self.lock = threading.RLock()
        self._stopped = threading.Event()

        self.S3Connection = S3Connection
        self._lock = multiprocessing.RLock()

    def __del__(self):
        if self._update_timer and self._update_timer.is_alive():
            self._update_timer.cancel()
        super(S3DirectiveWatcher, self).__del__()

    def run(self):

        self._stopped.clear()
        while not self._stopped.is_set():
            last_woke_up = datetime.datetime.utcnow()

            try:
                with self.activity_watcher.activate():
                    self.read_directives()
            except Exception as e:
                _log.exception('Uncaught exception when reading %s' % e)
                statemon.state.increment('reading_error')

            self._stopped.wait(options.interval -
                               (datetime.datetime.utcnow() -
                                last_woke_up).total_seconds())

    def stop(self):
        '''Stop this thread safely and allow it to finish what is is doing.'''
        self._stopped.set()

    def stop_handler(self, signum, frame):
        self.stop()
        sys.exit(-signum)

    def _update_time_last_reading(self):
        statemon.state.time_since_last_read = (
            datetime.datetime.utcnow() -
            self.last_update_time).total_seconds()

        self._update_timer = threading.Timer(
            10.0, self._update_time_last_reading)
        self._update_timer.daemon = True
        self._update_timer.start()

    @tornado.gen.coroutine
    def read_directives(self):
        # Create the connection to S3
        s3conn = self.S3Connection()
        try:
            bucket = s3conn.get_bucket(options.s3_bucket)
        except boto.exception.BotoServerError as e:
            _log.error('Could not get bucket %s: %s' %
                       (options.s3_bucket, e))
            statemon.state.increment('reading_error')
            return
        except boto.exception.BotoClientError as e:
            _log.error('Could not get bucket %s: %s' %
                       (options.s3_bucket, e))
            statemon.state.increment('reading_error')
            return
        except socket.error as e:
            _log.error('Error connecting to S3: %s' % e)
            statemon.state.increment('reading_error')
            return

        # getting the last hour
        now = datetime.datetime.utcnow()
        bucket_listing = bucket.list(
            prefix=self.last_update_time.strftime('%Y%m%d%H'))

        def compare_dates(date1, date2):
            date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
            if (datetime.datetime.strptime(date1, date_format) >= date2):
                return True
            return False

        # filter file by last modified
        filename_list = [
            i for i in bucket_listing
            if compare_dates(i.last_modified, self.last_update_time)
        ]

        # getting the current hour
        if (self.last_update_time.hour < now.hour):
            bucket_listing = bucket.list(prefix=now.strftime('%Y%m%d%H'))
            for key in bucket_listing:
                filename_list.append(key)

        # for each file get in S3
        for rs in filename_list:
            # parse the file to json
            file_content = bucket.get_key(rs.name).get_contents_as_string()
            parsed = self.parse_directive_file(file_content)

            for key, value in parsed.iteritems():
                #  get video metada
                vmd = yield tornado.gen.Task(
                    neondata.VideoControllerMetaData.get,
                    value['aid'], value['vid'])
                if vmd:
                    api_key = vmd.get_api_key()
                    # for each experiment - update directives
                    for i in vmd.controllers:
                        if i['state'] == neon_controller.ControllerExperimentState.COMPLETE:
                            continue
                        if rs.last_modified <= i['last_process_date']:
                            continue

                        ctr = yield tornado.gen.Task(
                            neon_controller.Controller.get,
                            i['controller_type'],
                            api_key, i['platform_id'])

                        try:
                            state = yield tornado.gen.Task(
                                ctr.update_experiment_with_directives,
                                i, value)

                            # update controller - last process date
                            vmd.update_controller(
                                i['controller_type'], i['platform_id'],
                                i['experiment_id'], i['video_id'],
                                state, time.time(), i['extras'])
                            yield tornado.gen.Task(vmd.save)

                        except ValueError as e:
                            _log.error("key=read_directives"
                                       " msg=controller api call failed")
                            _log.error(e.message)

        # update time
        self.last_update_time = datetime.datetime.utcnow()
        return

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
                key = neondata.VideoControllerMetaData._generate_subkey(
                    data['aid'], data['vid'])
                directives[key] = data

        return directives


def main(activity_watcher=utils.ps.ActivityWatcher()):
    # validate min and max values to interval option
    if (options.interval < 60) or (options.interval > 1200):  # 1 min or 20 min
        raise Exception("Invalid interval argument")
        return

    with activity_watcher.activate():
        dir_watcher = S3DirectiveWatcher(
            activity_watcher=activity_watcher)
        dir_watcher.start()

    atexit.register(tornado.ioloop.IOLoop.current().stop)
    signal.signal(signal.SIGTERM, dir_watcher.stop_handler)
    signal.signal(signal.SIGINT, dir_watcher.stop_handler)

    def update_directive_watcher_time():
        statemon.state.last_directive_watcher_update_time = (
            datetime.datetime.utcnow() -
            dir_watcher.last_update_time).total_seconds()
    tornado.ioloop.PeriodicCallback(update_directive_watcher_time, 10000)
    tornado.ioloop.IOLoop.current().start()
    dir_watcher.join(300)

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()

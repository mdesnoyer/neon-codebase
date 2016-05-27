#!/usr/bin/env python

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
import argparse
from model.predictor import DeepnetPredictor

import cv2
import threading
import logging
from time import time

from itertools import cycle
_log = logging.getLogger(__name__)

class StaticGetIp(object):
    def __init__(self, ip):
        self.ip = ip
    def get_ip(self, force_refresh=True):
        return self.ip

if __name__ == '__main__':

    counts = {
        'pending': 0,
        'completed': 0,
        'future_exception': 0, 
        'unknown_error': 0,
        'total_time': 0,
        'requested': 0,
        'response': 0,
        'peak_ips': 0
    }
    average_times = []
    pending = []
    completed = []
    requested = []

    parser = argparse.ArgumentParser()
    parser.add_argument('input_path', help='file with one local image path per line')
    parser.add_argument('--ip', default='localhost')
    parser.add_argument('--port', '-p', type=int, default=9000)
    parser.add_argument('--concurrency', '-c', type=int, default=10)
    parser.add_argument('--poll_interval', type=int, default=100)
    parser.add_argument('--quiet', '-q', dest='quiet', action='store_true', default=False)
    parser.add_argument('--until', '-u', type=int, default=1000)
    args = parser.parse_args()

    conn = StaticGetIp(args.ip)

    pred = DeepnetPredictor(
       concurrency=args.concurrency,
       aquila_connection=conn)

    paths = open(args.input_path, 'r').read().strip().split('\n')
    images = [cv2.imread(path) for path in paths]
    image_count = len(images)
    lock = threading.Condition()

    if not args.quiet:
        print('Starting with ip:{ip} port:{port} conc:{conc} path:{path} imgct:{imgct} poll every {interval}'.format(
            ip=args.ip,
            port=args.port,
            conc=args.concurrency,
            path=args.input_path,
            imgct=image_count,
            interval=args.poll_interval))

    def shutdown():
        if not args.quiet:
            print 'Writing logs'
        with open('/tmp/pending_log', 'w') as f:
            f.write('\n'.join([str(x) for x in pending]))
        with open('/tmp/completed_log', 'w') as f:
            f.write('\n'.join([str(x) for x in completed]))
        with open('/tmp/requested_log', 'w') as f:
            f.write('\n'.join([str(x) for x in requested]))
        with open('/tmp/counts_dict_log', 'w') as f:
            f.write(str(counts))
        print '%d imgs (%d errors) in %.1fs: peak: %.2fi/s mean: %.2fs/i' % (counts['response'], counts['unknown_error'] + counts['future_exception'], time() - start_time, counts['peak_ips'], counts['total_time'] / max(1, counts['completed']))

    def cb(future, request_time, start_time):
        exo_exception = None
        try: 
            result = future.result()
            exception = future.exception()
        except Exception as e:
            exo_exception = e
        elapsed_time = time() - start_time
        response_time = time() - request_time
        with lock:
            counts['pending'] -= 1
            counts['response'] += 1
            if exo_exception:
                counts['unknown_error'] += 1
            elif exception:
                counts['future_exception'] += 1
            elif result:
                counts['completed'] += 1
                counts['total_time'] += response_time
            if not counts['response'] % args.poll_interval:
                rate = counts['completed'] / elapsed_time
                if rate > counts['peak_ips']:
                   counts['peak_ips'] = rate
                mean_compl_time = counts['total_time'] / max(1, counts['completed'])
                out = '%i completed in %.1fs, %i error state, rate: %.2fi/s mean: %.2fs/i, most recent: %.2fsec'
                if not args.quiet:
                    print out % (counts['completed'], elapsed_time, counts['future_exception'] + counts['unknown_error'], 
                                 rate, mean_compl_time, response_time)
                average_times.append(mean_compl_time)
                pending.append(counts['pending'])
                completed.append(counts['completed'])
                requested.append(counts['requested'])
            lock.notify()
      
    try:
        start_time = time()
        for i in cycle(images):
            with lock:
                if args.until and counts['requested'] >= args.until:
                    break
                counts['pending'] += 1
                counts['requested'] += 1
            request_time = time()
            r = pred.predict(i)
            cbl = lambda future, request_time=request_time, start_time=start_time: cb(future, request_time, start_time)
            r.add_done_callback(cbl)
        with lock:
            while counts['pending']:
                lock.wait()
        raise KeyboardInterrupt

    except KeyboardInterrupt:
        shutdown()

        try:
            pred.shutdown()
        except AttributeError:
            pass

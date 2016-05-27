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

_log = logging.getLogger(__name__)

class StaticGetIp(object):
    def __init__(self, ip):
        self.ip = ip
    def get_ip(self, force_refresh=True):
	return self.ip

if __name__ == '__main__':

    counts = {
        'request': 0,
        'response': 0,
        'exception': 0,
        'elapsed': 0
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('input_path', default='/data/targ/images')
    parser.add_argument('--ip', default='localhost')
    parser.add_argument('--port', default=9000)
    parser.add_argument('--concurrency', default=10)
    parser.add_argument('--poll_interval', default=100)
    args = parser.parse_args()

    conn = StaticGetIp(args.ip)

    pred = DeepnetPredictor(
       concurrency=args.concurrency,
       aquila_connection=conn)

    paths = open(args.input_path, 'r').read().strip().split('\n')
    images = [cv2.imread(path) for path in paths]
    image_count = len(images)
    lock = threading.Condition()

    print('Starting with ip:{ip} port:{port} conc:{conc} path:{path} imgct:{imgct} interval:{interval}'.format(
        ip=args.ip,
        port=args.port,
        conc=args.concurrency,
        path=args.input_path,
        imgct=image_count,
        interval=args.poll_interval))

    def cb(future, request_time):
        try: 
            result = future.result()
            exception = future.exception()
        except Exception as e:
            exception = e
        response_time = time() - request_time
        with lock:
            counts['response'] += 1
            counts['elapsed'] += response_time
            if not counts['response'] % args.poll_interval:
               print('%s avg:%ss last:%ss' % (counts, counts['elapsed']/counts['response'], response_time))
            if exception:
                print(exception)
                counts['exception'] += 1

    while True:
        with lock:
            counts['request'] += 1
            current = counts['request']
        r = pred.predict(images[current % image_count])
        cbl = lambda future, request_time = time(): cb(future, request_time)
        r.add_done_callback(cbl)

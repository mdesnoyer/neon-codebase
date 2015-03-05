#!/usr/bin/env python

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.exception
from boto.s3.connection import S3Connection
from cmsdb import neondata
from collections import deque
import datetime
import hashlib
import json
import logging
import multiprocessing
import os
import Queue
import random
import re
import redis
import time
import tornado.httpserver
import tornado.gen
import tornado.ioloop
import tornado.web
import tornado.escape
import threading
import utils.botoutils
import utils.http
import utils.neon
import utils.ps
from utils import statemon

AWS_REGION = 'us-west-2'
AWS_ACCESS_KEY_ID = 'AKIAIHEAXZIPN7HC5YBQ'
AWS_SECRET_ACCESS_KEY = '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'

#
video_server_ip = '10.1.1.1'
video_server_port = 80

#
INITIAL_SLEEP = 0

# to indicate that the process should quit
shutdown = false

# 
high_water_mark = 80 
low_water_mark = 10


vclients = None

def get_queue_size():
    return 50 

def fetch_all_vclients_status():
    
    conn = boto.ec2.connect_to_region(AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    if conn == None
        return None

    # only inquire about video clients
    reservations = conn.get_all_instances(ifilters={"tag:opsworks:layer:vclient" : "Video Client"})
    
    # all instances
    vclients = [i for r in reservations for i in r.instances]


def scaling_operations_in_progress()
    return true

def handle_low_load():
    pass

def handle_high_load():
    pass


def run():

    sleep_time = INITIAL_SLEEP 

    
    while(shutdown == false):

        # sleep
         time.sleep(sleep_time)

        # get all video clients states
        fetch_all_vclients_status()

        # if any instance either booting or shutting down
        if(scaling_operations_in_progress() == true):
            continue

        # get queue size
        queue_size = get_queue_size() 

        # error
        if(queue_size < 0):
            continue

        # the case where the video server queue is growing
        if(queue_size > queue_high_water_mark):
            handle_high_load()

        # the case where the video server queue is shrinking
        else if(queue_size < queue_low_water_mark):
            handle_low_load()

        # else the queue is normal
        else:        
    


def main():
    run()


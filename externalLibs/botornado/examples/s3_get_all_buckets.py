#!/usr/bin/env python

import os
import sys
import time
import tornado.ioloop
import tornado.options

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # FIXME
import botornado.s3
from botornado.s3.connection import AsyncS3Connection

tornado.options.parse_command_line(sys.argv)

a = 'AKIAIHY5JHBE2BTFR4SQ'
s = '1ByiJJdZIuKTUfH7bqfJ8spmopEB8AUgzjpdSyJf'
s3 = AsyncS3Connection(aws_access_key_id= a,
    aws_secret_access_key=s)

def get_all_buckets():
  def cb(response):
    print response
    ioloop.stop()
    sys.exit(0)
  s3.get_all_buckets(callback=cb)

ioloop = tornado.ioloop.IOLoop.instance()
get_all_buckets()
#ioloop.add_timeout(time.time(), get_all_buckets)
ioloop.start()

# vim:set ft=python :

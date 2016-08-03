#!/usr/bin/env python
'''
    Script that listens for changes in our Redis store and 
        moves it to Postgres  
''' 
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

#import multiprocessing 
#import Queue 
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.gen
import tornado.httpclient
from tornado.queues import Queue
#from change_listener_producer import Producer
#import change_listener_producer
import change_listener_consumer
import change_listener_producer
import utils
import utils.neon

#if __name__ == '__main__': 
@tornado.gen.coroutine
def main(): 
    queue = Queue() 
    #tornado.ioloop.IOLoop.current().spawn_callback(lambda: change_listener_consumer.consumer(queue)) 
    #tornado.ioloop.IOLoop.current().add_callback(lambda: change_listener_consumer.consumer(queue))
     
    #yield change_listener_producer.producer(queue) 
    yield change_listener_consumer.consumer(queue) 
    yield queue.join()  
if __name__ == '__main__':
    utils.neon.InitNeon()
    #main()  
    tornado.ioloop.IOLoop.current().run_sync(main)
    #tornado.ioloop.IOLoop.current().run_async(main)

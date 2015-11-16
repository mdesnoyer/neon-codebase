import boto.exception
import boto.sqs
import boto.sqs.queue
from boto.sqs.message import Message
from boto.s3.connection import S3Connection
from collections import deque
import concurrent.futures
import datetime
import hashlib
import json
import multiprocessing
import os
import Queue
import random
import re
import redis
import time
import tornado
from tornado import ioloop
from tornado.concurrent import run_on_executor
import utils.botoutils
import utils.http
import utils.ps

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('num_queues', default=3, help='Number of queues in the SQS server')
define('queue_prefix', default="Priority", type=str,
       help="The prefix of the name of each queue")

class VideoProcessingQueue(object):
    '''Replaces the current server code with an AWS SQS instance'''
    def __init__(self, region, access_key, secret_key, timeout=30):
        '''Connect to AWS and creates N SQS queues 

        Inputs:
        region - the region the server resides in
        access_key - the AWS access access_key
        secret_key - the AWS secret_key
        timeout (optional) - sets the visibility timeout for all the messages
                             in the queue. Default value is 30 seconds

        Returns:
        Nothing, but creates pointers to the SQS queues
        '''
        self.conn = boto.sqs.connect_to_region(
                    region, aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)
       
        self.queue_list = []
        self.max_priority = 0.0
        self.cumulative_priorities = []

        self.executor = concurrent.futures.ThreadPoolExecutor(10)
        self.io_loop = ioloop.IOLoop.current()

        for i in range(options.num_queues):
            new_queue = self._create_queue(options.queue_prefix + str(i), timeout)
            self.queue_list.append(new_queue)
            '''The next two lines define how the queues are picked. 
            Each new queue is half as likely to be selected as the previous one.
            When selecting a queue (in _get_priority_qindex), a random uniform
            number is generated and it is checked against these values. The range
            it lies in defines which queue is selected. 
            '''
            self.max_priority += 1.0/2**(i)
            self.cumulative_priorities.append(self.max_priority)

    def _create_queue(self, queue_name, timeout):
        '''Checks to see if the queue exists before creating it

        Inputs:
        queue_name - the name of the queue to lookup/creates
        timeout - sets the visibility timeout for all the messages in the queue. 
                  Default value is 30 seconds

        Returns:
        The queue object
        '''
        queue = self.conn.lookup(queue_name)
        if queue:
            return queue
        return self.conn.create_queue(queue_name, timeout)

    @run_on_executor
    def _get_priority_qindex(self):
        '''Uses a random uniform distribution to pick a queue
           It is biased towards queues with a higher priority

           Inputs:
           None

           Returns:
           An index, which is used to point to the priority of the queue
           The value of the index is the same value as the priority of the queue
        '''
        priority = random.uniform(0, self.max_priority)
        for index in range(len(self.cumulative_priorities)):
            if priority < self.cumulative_priorities[index]:
                return index
   
    @run_on_executor
    def _get_queue(self, priority):
        '''Returns the queue with the given priority

           Inputs:
           priority - the priority of the queue

           Returns:
           The queue object that corresponds to the queue of the given priority
        '''
        return self.queue_list[priority]

    @run_on_executor    
    def _add_attributes(self, priority, message, timeout):
        '''Adds priority information to the message. This way, the client that
           reads the message does not need to know about the priority information
           of the queue it comes from, or even that such information exists.

           Inputs:
           priority - the priority of the queue the message is being written to
           message - the message to be written
           timeout - the visibility timeout of the message

           Returns:
           The modified message object
        '''
        message.message_attributes = {
                  "priority": {
                        "data_type": "Number",
                        "string_value": str(priority)
                 }, "duration": {
                        "data_type": "Number",
                        "string_value": str(timeout)
                 }
        }
        return message

    @run_on_executor
    def _change_message_visibility(self, message, queue, timeout):
        '''Changes the visiblity of the message based on the information in 
           the body of the message.

           Inputs:
           message - the message to be modified
           queue - the queue the message is residing in
           timeout - changes the visibility_timeout of the message. The 
                     visibility_timeout defines how much time must pass before 
                     the message can be read again.

            Returns:
            Void
        '''
        self.conn.change_message_visibility(queue, message.receipt_handle,
                                            timeout)

    @tornado.gen.coroutine
    def write_message(self, priority, message, timeout=300):
        '''Writes a message to the specified priority queue. The priority and
           durations attributes are written to the message first by calling 
           _add_attributes

           Inputs:
           priority - the priority of the queue to be written to
           message - the message to be written
           timeout (optional) - the visibility timeout of the message,
                                default is 300 seconds (5 minutes)

           Returns:
           The message if successful, False otherwise
        '''
        queue = yield self._get_queue(priority)
        message = yield self._add_attributes(priority, message, timeout)
        final_message = queue.write(message)
        raise tornado.gen.Return(final_message)

    @tornado.gen.coroutine
    def read_message(self):
        '''Picks a random queue to read from, using the fairweighted priority.
           This random selection resides in _get_priority_qindex.
           If the queue is empty (returns None instead of a message) it goes to
           the queue with the next lowest priority until they are all exhausted

           Inputs:
           None

           Returns:
           A message if successful, None otherwise
        '''
        priority = yield self._get_priority_qindex()
        message = None
        while priority < options.num_queues and message == None:
            queue = yield self._get_queue(priority)
            message = queue.read(message_attributes=['All'])
            priority += 1

        timeout = 300
        if(message):
            if(message.message_attributes['duration']['string_value']):
                timeout = int(message.message_attributes['duration']['string_value'])
            yield self._change_message_visibility(message, queue, timeout)
        raise tornado.gen.Return(message)

    @tornado.gen.coroutine
    def delete_message(self, message):
        '''Deletes the specified message

           Inputs:
           message - the message object to be deleted from the queue

           Returns:
           True if successful, False otherwise
        '''
        priority = int(message.message_attributes['priority']['string_value'])
        queue = yield self._get_queue(priority) 
        deleted_message = queue.delete_message(message)
        raise tornado.gen.Return(deleted_message)

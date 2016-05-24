import boto
import boto.exception
import boto.sqs
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
from utils import statemon

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('num_queues', default=3, help='Number of queues in the SQS server')
define('queue_prefix', default="Priority_", type=str,
       help="The prefix of the name of each queue")
define('default_timeout', default=300, help='Default timeout for a message')
define('region', default='us-east-1', help='region where the queue resides')

statemon.define('write_failure', int)
statemon.define('read_failure', int)
statemon.define('delete_failure', int)

class VideoProcessingQueue(object):
    '''Replaces the current server code with an AWS SQS instance'''
    def __init__(self):
        '''Set up the basic variables 

        Inputs:
        None

        Returns:
        None
        '''
        self._reset()

        self.executor = concurrent.futures.ThreadPoolExecutor(10)

    def _reset(self):
        self.queue_list = []
        self.max_priority = 0.0
        self.cumulative_priorities = []

        self.region = None
        self.queue_prefix = None
        self.conn = None

    def __del__(self):
        self.executor.shutdown(False)
        
    @tornado.gen.coroutine
    def _connect_to_server(self, timeout=options.default_timeout):
        '''Connect to AWS and creates N SQS queues (if necessary)
        
        Inputs:

        timeout (optional) - sets the visibility timeout for all the messages
                             in the queue. Default value is 30 seconds

        Returns:
        Nothing, but creates pointers to the SQS queues
        '''
        if (self.region is None or
            self.region != options.region or
            self.queue_prefix != options.queue_prefix or
            options.num_queues != len(self.queue_list)):

            self._reset()
            
            yield self._create_sqs_server(options.region)

            self.region = options.region
            self.queue_prefix = options.queue_prefix

            for i in range(options.num_queues):
                queue_name = '%s%i' % (self.queue_prefix, i)
                new_queue = yield self._create_queue(
                    queue_name,
                    timeout)
                self.queue_list.append(new_queue)
                
                # The next two lines define how the queues are picked.
                # Each new queue is half as likely to be selected as
                # the previous one.  When selecting a queue (in
                # _get_priority_qindex), a random uniform number is
                # generated and it is checked against these
                # values. The range it lies in defines which queue is
                # selected.
                
                self.max_priority += 1.0/2**(i)
                self.cumulative_priorities.append(self.max_priority)

            self.region = options.region
            self.queue_prefix = options.queue_prefix

            _log.info("Connected to SQS server on region %s" % options.region)
    
    @run_on_executor
    def _create_sqs_server(self, region):
        '''Creates to/connects to the server

        Inputs:
        region - the region the server resides in

        Returns:
        Nothing, but creates the connection to the SQS system
        '''
        self.conn = boto.sqs.connect_to_region(
                    region)

    @run_on_executor
    def _create_queue(self, queue_name, timeout):
        '''Checks to see if the queue exists before creating it

        Inputs:
        queue_name - the name of the queue to lookup/creates
        timeout - sets the visibility timeout for all the messages in the queue. 
                  Default value is 30 seconds (this is the default value as 
                  defined by _connect_to_server, which is what calls this def)

        Returns:
        The queue object
        '''
        queue = self.conn.lookup(queue_name)
        if queue:
            return queue
        return self.conn.create_queue(queue_name, timeout)

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
   
    def _get_queue(self, priority):
        '''Returns the queue with the given priority

           Inputs:
           priority - the priority of the queue

           Returns:
           The queue object that corresponds to the queue of the given priority
        '''
        return self.queue_list[priority]
 
    def _add_attributes(self, priority, message, duration):
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
                        "string_value": str(duration)
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

    @run_on_executor
    def _sqs_write(self, queue, message):
        return queue.write(message)

    @run_on_executor
    def _sqs_read(self, queue):
        return queue.read(message_attributes=['All'])

    @run_on_executor
    def _sqs_delete(self, queue, message):
        return queue.delete_message(message)

    @run_on_executor
    def _sqs_count(self, queue):
        return queue.count()

    @tornado.gen.coroutine
    def size(self):
        '''Return the approximate size of the queue.'''
        yield self._connect_to_server()
        count = 0
        for q in self.queue_list:
            x = yield self._sqs_count(q)
            count += x

        raise tornado.gen.Return(count)

    @tornado.gen.coroutine
    def write_message(self, priority, message_body, duration=None):
        '''Writes a message to the specified priority queue. The priority and
           durations attributes are written to the message first by calling 
           _add_attributes

           Inputs:
           priority - the priority of the queue to be written to
           message_body - the body of the message to be written
           duration - Duration of the video if known

           Returns:
           The message body if successful, False otherwise
        '''
        yield self._connect_to_server()
        try:
            if priority >= options.num_queues:
                raise ValueError('Invalid Priority. The valid range is 0 to %s' 
                                 % str(options.num_queues - 1))
            if not isinstance(message_body, basestring):
                raise ValueError('Message_body must be string, instead got %s'
                                 % type(message_body))
            queue = self._get_queue(priority)
            message = Message()
            message.set_body(message_body)
            message = self._add_attributes(priority, message, duration)
            final_message = yield self._sqs_write(queue, message)
            raise tornado.gen.Return(final_message.get_body())
        except tornado.gen.Return:
            raise tornado.gen.Return(final_message.get_body())
        except ValueError, e:
            statemon.state.increment('write_failure')
            raise

    @tornado.gen.coroutine
    def read_message(self):
        '''Picks a random queue to read from, using the fairweighted priority.
           This random selection resides in _get_priority_qindex.
           If the queue is empty (returns None instead of a message) it goes to
           the queue with the next lowest priority until they are all exhausted

           Inputs:
           None

           Returns:
           A boto.sqs.Message if successful, None otherwise
        '''
        yield self._connect_to_server()
        priority = self._get_priority_qindex()
        message = None
        while priority < options.num_queues and message is None:
            queue = self._get_queue(priority)
            message = yield self._sqs_read(queue)
            priority += 1
        raise tornado.gen.Return(message)

    @tornado.gen.coroutine
    def delete_message(self, message):
        '''Deletes the specified message

           Inputs:
           message - the message object to be deleted from the queue

           Returns:
           True if successful, False otherwise
        '''
        yield self._connect_to_server()
        try:
            priority = int(message.message_attributes['priority']['string_value'])
            queue = self._get_queue(priority)
            deleted_message = yield self._sqs_delete(queue, message)
            raise tornado.gen.Return(deleted_message)
        except tornado.gen.Return:
            raise tornado.gen.Return(deleted_message)
        except Exception, e:
            statemon.state.increment('delete_failure')
            raise Exception(e.message)

    @tornado.gen.coroutine
    def hide_message(self, message, timeout):
        '''Hides a message for timeout so that other workers won't see it.

        Inputs:
        message - The message to hide
        timeout - The length of time in seconds to hide it.
        '''
        yield self._connect_to_server()
        priority = int(message.message_attributes['priority']['string_value'])
        queue = self._get_queue(priority)
        yield self._change_message_visibility(queue, message, timeout)

    def get_duration(self, message):
        '''Returns the duration of the job encoded in the message.

        Returns None if the duration is unknown.
        '''
        try:
            return int(message.message_attributes['duration']['string_value'])
        except KeyError:
            return None
        except ValueError:
            return None

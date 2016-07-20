'''
Copyright Josh Gachnang 2012, josh@servercobra.com, released under the New BSD License.

SQS Mock Python is a mock implementation of the Amazon SQS API. It implements (most) of the API for Connections and Queues, and returns the appropriate values for API calls. 
It works by holding the queue on disk, and implementing the API calls to work with a file per queue, rather than a distributed system. Unlike SQS, it is able to work in real
time. When an object is added to the queue, it is instantly available, making testing much easier. Applications can simply make calls to the SQSConnectionMock class instead 
of the SQSConnection class for testing purposes. This is only desirable if the application you are writing is tolerant of the delays inherent in SQS. If not, you should
use something like RabbitMQ instead. 

Modified by Sunil
Modified by Ahmaidan

'''
from boto.sqs.message import Message
import copy
import random
from random import randrange
import threading
import uuid

import logging
_log = logging.getLogger(__name__)

class SQSQueueMock(object):
    '''
    SQS Mock Queue class
    '''
    name = None  
    def __init__(self, name):
        self.lock = threading.RLock()
        self.name = name

        self.message_queue = []
        
    #Deprecated function           
    def clear(self, page_size=10, vtimeout=10):
        try:
            del self.message_queue[:]
        except EOFError:
            return False
        return True
           
    def count(self, page_size=10, vtimeout=10):
        count = len(self.message_queue)
        return count
               
    def count_slow(self, page_size=10, vtimeout=10):
        return self.count(page_size=10, vtimeout=10)
       
    def delete(self):
        raise NotImplementedError()
      
    def delete_message(self, message):
        with self.lock:
            try:
                for x in range(0, len(self.message_queue)):
                    if(message.receipt_handle == self.message_queue[x].receipt_handle):
                        message = self.message_queue.pop(x)
                        return True
            except ValueError:
                return False
        return False
       
    def get_messages(self, num_messages=1, visibility_timeout=None, attributes=None):
        messages = []

        i = 0
        while i < num_messages:
            with self.lock:
                try:
                    msg = self.message_queue[i]
                    messages.append(msg)
                except IndexError:
                    pass
                i += 1
        return messages
       
    def read(self, visibility_timeout=None, message_attributes=None):
        with self.lock:
            if self.message_queue:
                msg = copy.deepcopy(self.message_queue[0])
                all_attrs = msg.message_attributes
                msg.message_attributes = {}
                if (message_attributes and
                    message_attributes[0] != 'All' and
                    message_attributes[0] != ['.*']):
                    for attr in message_attributes:
                        msg.message_attributes[attr] = all_attrs[attr]
                else:
                    msg.message_attributes = all_attrs
                return msg
                        
            else:
                return None
    
    def write(self, message):
        try:
            with self.lock:
                message.receipt_handle = uuid.uuid4()
                self.message_queue.append(message)
        except IndexError:
            raise Exception()
        return message
    
class SQSConnectionMock(object):
    def __init__(self):
        self.queue_dict = {}
        self.regions = ['us-east-1', 'us-west-2', 'us-west-1',
                        'eu-west-1', 'eu-central-1', 'ap-southeast-1',
                        'ap-southeast-2', 'ap-northeast-1', 'sa-east-1']

    def get_queue(self, queue_name):
        try:
            return self.queue_dict[queue_name]
        except SyntaxError:
            return None
        except KeyError:
            return None
             
    def get_all_queues(self, prefix=""):
        return self.queue_dict.values()
        
    def delete_queue(self, queue, force_deletion=False):
        try:
            del self.queue_dict[queue.name]
        except KeyError:
            pass
                
    def delete_message(self, queue, message):
        return queue.delete_message(message)
               
    def create_queue(self, name, visibility_timeout=None):
        try:
            q = self.queue_dict[name]
        except KeyError as e:
            q = SQSQueueMock(name)
        self.queue_dict[name] = q
        return q

    def lookup(self, name):
        return self.queue_dict.get(name, None)

    #TODO: change this so it can actually approximate the functionality        
    def change_message_visibility(self, queue, receipt_handle, timeout):
        if queue not in self.queue_dict.values():
            raise ValueError('Must be a known queue')
        elif not isinstance(receipt_handle, basestring):
            raise ValueError('Must be a valid receipt handle')
        float(timeout)
        return True

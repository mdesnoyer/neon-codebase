'''
Copyright Josh Gachnang 2012, josh@servercobra.com, released under the New BSD License.

SQS Mock Python is a mock implementation of the Amazon SQS API. It implements (most) of the API for Connections and Queues, and returns the appropriate values for API calls. 
It works by holding the queue on disk, and implementing the API calls to work with a file per queue, rather than a distributed system. Unlike SQS, it is able to work in real
time. When an object is added to the queue, it is instantly available, making testing much easier. Applications can simply make calls to the SQSConnectionMock class instead 
of the SQSConnection class for testing purposes. This is only desirable if the application you are writing is tolerant of the delays inherent in SQS. If not, you should
use something like RabbitMQ instead. 

Modified by Sunil
Modified by Hmaidan

'''
from boto.sqs.message import Message
import random
from random import randrange
import threading

import logging
_log = logging.getLogger(__name__)

class SQSQueueMock(object):
    '''
    SQS Mock Queue class
    '''
    name = None  
    def __init__(self, filename, create=False, num_queues=3):
        self.lock = threading.RLock()

        self.attributes = {}
        self.queue_list = []
        #self.out_stream = cStringIO.StringIO()
        self.message_queue = []
        self.num_queues = num_queues

        for x in range(0, num_queues):
            self.queue_list.append(self.message_queue)

        self.name = filename
        
    #Deprecated function           
    def clear(self, page_size=10, vtimeout=10):
        try:
            del self.message_queue[:]
        except EOFError:
            return False
        return True
           
    def count(self, page_size=10, vtimeout=10):
        count = 0
        for x in range(0, self.num_queues):
            #in_stream = cStringIO.StringIO(self.queue_list[x].getvalue())
            self.message_queue = self.queue_list[x]
            count += len(self.message_queue)
        return count
               
    def count_slow(self, page_size=10, vtimeout=10):
        return self.count(page_size=10, vtimeout=10)

    #Assumption: delete is only called to delete the current queue          
    def delete(self):
        try:
            del self.message_queue[:]
        except OSError:
            return False
        return True
      
    def delete_message(self, message):
        priority = 0
        if(message.message_attributes):
            priority = int(message.message_attributes['priority']['string_value'])
        self.message_queue = self.queue_list[priority]
        with self.lock:
            try:
                for x in range(0, len(self.message_queue)):
                    if(message.get_body() == self.message_queue[x].get_body()):
                        message = self.message_queue.pop(x)
                        return True
            except ValueError:
                return False
        return False
       
    def get_messages(self, num_messages=1, visibility_timeout=None, attributes=None):
        attributes_to_return = []
        if attributes is not None:
            attributes_to_return = attributes.split(',')
        messages = []

        i = 0
        while i < num_messages and len(prev_data) > 0:
            try:
                msg = self.message_queue(i)
                for attr in attributes_to_return:
                    try:
                        msg.attributes[attr] = self.attributes[attr]
                    except KeyError:
                        pass
                messages.append(msg)
            except IndexError:
                pass
            i += 1
        return messages
       
    def read(self, visibility_timeout=None, message_attributes=['All']):
        random_index = 0
        while(random_index < self.num_queues):
            self.message_queue = self.queue_list[random_index]
            with self.lock:
                try:
                    message = self.message_queue[0]
                    return message
                except IndexError:
                    #Do nothing, loop again
                    random_index += 1
        return None
    
    def write(self, message):
        priority = 0
        if(message.message_attributes):
            priority = int(message.message_attributes['priority']['string_value'])
        try:
            with self.lock:
                self.message_queue = self.queue_list[priority]
                self.message_queue.append(message)
        except IndexError:
            return False
            
        return message
    
class SQSConnectionMock(object):   
    def get_queue(self, queue):
        try:
            return SQSQueueMock(queue)
        except SyntaxError:
            return None
             
    def get_all_queues(self, prefix=""):
        return SQSQueueMock.queue_list
        
    def delete_queue(self, queue, force_deletion=False):
        q = self.get_queue(queue)
        if q.count() != 0:
            # Can only delete empty queues
            return False
        return q.close()
                
    def delete_message(self, queue, message):
        return queue.delete_message(message)
               
    def create_queue(self, name, visibility_timeout=None):
        q = SQSQueueMock(name, create=True)
        return q

    #As the memory streams aren't meant to be persistent, this should always
    #return None
    def lookup(self, name):
        return None

    #TODO: change this so it can actually approximate the functionality        
    def change_message_visibility(self, queue, receipt_handle, timeout):
        return True

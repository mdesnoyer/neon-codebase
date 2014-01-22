'''Tools for synchronization

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
  sys.path.insert(0,base_path)

import functools
import tornado.ioloop

def optional_sync(func):
    '''A decorator that makes an asyncronous function optionally synchronous.

    If you have an asynchronous function that has a special "callback"
    argument, this decorator will change "callback" so that it's
    optional. If callback is None, then the function will be
    synchronous. If it is not None, the function will be asyncronous.

    To use it, make sure this decorator is on the outside. For example
    @optional_sync
    @tornado.gen.coroutine
    def do_something_async(url):
      response = yield tornado.httpclient.AsyncHTTPClient().fetch(url)
      raise tornado.gen.Return(random.shuffle(response))

    Then, a synchronous call would look like:
    weird_response = do_something_async('http://hi.com')

    And an asynchronous call would be:
    do_something_async('http://hi.com', callback=process_response)

    or

    weird_response = yield tornado.gen.Task(do_something_async,
    'http://hi.com')

    Note that technically, you're not guaranteed to have a purely
    synchronous function. The current thread's ioloop is used so if
    there are other things registered on that ioloop, they can run
    too.
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'callback' in kwargs:
            if kwargs['callback'] is not None:
                return func(*args, **kwargs)
            kwargs.pop('callback')
        return tornado.ioloop.IOLoop.current().run_sync(
            lambda : func(*args, **kwargs))

    return wrapper

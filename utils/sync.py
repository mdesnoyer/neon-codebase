'''Tools for synchronization

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
  sys.path.insert(0, __base_path__)

import contextlib
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

    or if it is in a @tornado.gen.coroutine, 

    weird_response = yield tornado.gen.Task(do_something_async,
    'http://hi.com')

    Note that inside the function, you must use
    tornado.ioloop.IOLoop.current() to get the current io
    loop. Otherwise it will hang.
    '''
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'callback' in kwargs:
            if kwargs['callback'] is not None:
                return func(*args, **kwargs)
            kwargs.pop('callback')

        with bounded_io_loop() as io_loop:
            return io_loop.run_sync(lambda : func(*args, **kwargs))

    return wrapper


@contextlib.contextmanager
def bounded_io_loop():
    '''This context manager allows you to have a new ioloop set as the
    current one.

    When the context manager is done, the last current io_loop is returned.

    Example:
    with bounded_io_loop() as ioloop:
      ioloop.run_sync()
    '''
    old_ioloop = tornado.ioloop.IOLoop.current()

    temp_ioloop = tornado.ioloop.IOLoop()
    temp_ioloop.make_current()

    try:
        yield temp_ioloop

    finally:
        old_ioloop.make_current()
        temp_ioloop.close()

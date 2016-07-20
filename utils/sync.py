'''Tools for synchronization

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
  sys.path.insert(0, __base_path__)

import concurrent.futures
import contextlib
import functools
import logging
import threading
import time
import tornado.gen
import tornado.httpclient
import tornado.ioloop

_log = logging.getLogger(__name__)

def optional_sync(func):
    '''A decorator that makes an asyncronous function optionally synchronous.

    If you have an asynchronous function that has a special "callback"
    argument, this decorator will change "callback" so that it's
    optional. If callback is None, then the function will be
    synchronous. If it is not None, the function will be asyncronous.

    Also, if your function returns a Future, then if you add the
    special "async=True" keywork argument, an asynchronous call will
    be made and the Future will be returned. This is useful to avoid a
    tornado.gen.Task wrapping.

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

    or if your async function returns a Future (like tornado.gen.coroutine):

    weird_response = yield do_something_async('http://hi.com', async=True)

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
        if 'async' in kwargs:
            async = kwargs['async']
            kwargs.pop('async')
            if async:
                return func(*args, **kwargs)

        with bounded_io_loop() as io_loop:
            retval = io_loop.run_sync(lambda : func(*args, **kwargs))
            if isinstance(retval, Exception):
                raise retval
            return retval

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
        # let's grab the httpclientinstance and 
        # close it explicitly so we don't leak memory
        client = tornado.httpclient.AsyncHTTPClient(
            io_loop=temp_ioloop)
        client.close() 
        temp_ioloop.close()

class IOLoopThread(threading.Thread):
    '''A thread that just runs an io loop.'''
    def __init__(self, name=None):
        super(IOLoopThread, self).__init__(name=name)
        self.io_loop = tornado.ioloop.IOLoop(make_current=False)

    def __del__(self):
        self.io_loop.close()

    def run(self):
        self.io_loop.make_current()
        self.io_loop.start()

    def stop(self):
        self.io_loop.stop()

class LockAquireThread(threading.Thread):
    '''A thread that will set a future when a lock is aquired.'''
    def __init__(self, lock):
        super(LockAquireThread, self).__init__()
        self.lock = lock
        self.future = concurrent.futures.Future()
        self.daemon = True

    def run(self):
        try:
            if not self.future.set_running_or_notify_cancel():
                return
            self.lock.acquire()
            self.future.set_result(True)
        except Exception as e:
            self.future.set_exception(e)
        

class FutureLock(object):
    '''Object that wrap a lock but returns a Future on aquire().

    Can be used to use syncronization primitives in coroutines. e.g.

    _lock = FutureLock(multiprocessing.Semaphore())

    yield _lock.acquire()
    try:
      do something
    finally:
      _lock.release()
    '''
    def __init__(self, lock):
        self.lock = lock

    def acquire(self):
        '''Exactly like normal acquire but returns a Future if it's not ready.'''
        if self.lock.acquire(False):
            # We have the lock
            future = concurrent.futures.Future()
            future.set_result(True)
            return future

        # We need to wait, so setup the future
        thread = LockAquireThread(self.lock)
        thread.start()
        return thread.future

    def release(self):
        self.lock.release()

class PeriodicCoroutineTimer(object):
    '''Class that acts exactly like tornado.ioloop.PeriodicCallback
    except it can take a coroutine as a function.
    '''
    def __init__(self, callback, callback_time, io_loop=None):
        '''Initializes a callback to run periodically.

        callback - Function to call. Can be a coroutine
        callback_time - Time in milliseconds
        io_loop - IOLoop to run on.
        '''
        self._ioloop = io_loop or tornado.ioloop.IOLoop.current()
        self._callback = callback
        self._callback_time = callback_time / 1000.
        self._stopped = True  # Is the time active or stopped
        self._running = False # Are we in the middle of a call?
        self._cur_future = None

    def start(self):
        if self._stopped:
            self._stopped = False
            self._ioloop.spawn_callback(self.run)

    def stop(self):
        self._stopped = True

    def is_running(self):
        '''Returns True if the timer is running.'''
        return not self._stopped

    @tornado.gen.coroutine
    def wait_for_running_func(self):
        '''Waits until the the currently funning functions is done.'''
        if self._running:
            val = yield self._cur_future
            raise tornado.gen.Return(val)

    @tornado.gen.coroutine
    def run(self):
        while not self._stopped:
            self._running = True
            try:
                start_time = time.time()

                self._cur_future = tornado.gen.maybe_future(self._callback())
                yield self._cur_future
            except Exception as e:
                _log.exception('Unexpected error when calling callback %s: %s'
                               % (self._callback, e))
            self._running = False
            wait_time = self._callback_time - (time.time() - start_time)
            if not self._stopped:
                yield tornado.gen.sleep(max(wait_time, 0.0))

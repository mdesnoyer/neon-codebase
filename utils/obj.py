'''Utilities for dealing with python objects.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import numpy as np

def full_object_str(obj, exclude=[]):
    '''Returns a JSON-like object string.

    For those fields in exclude, the data printed will be replaced by "..."
    '''
    field_strs = []
    for name, val in obj.__dict__.items():
        s = None
        if name in exclude:
            np_arr = np.array(val)
            if len(np_arr.shape) == 0:
                s = '%s: ...' % name
            else:
                s = '%s: <%s array>' % (name, str(np_arr.shape).replace(',','x'))
        else:
            s = '%s: %s' % (name, val)
        field_strs.append(s)
    return '<%s> {%s}' % (obj.__class__.__name__, ', '.join(field_strs))

class KeyedSingleton(type):
    '''A Singleton metaclass that is keyed by type and a key that is passed in.

    To use, set the __metaclass__ property of your class. e.g.

    class MyClass(BaseClass):
        __metaclass__ = utils.obj.Singleton

    Then, every time you call MyClass(key), you get the same object

    The key can be any python object that can be used as a key in a dictionary.
    '''
    _instances = {}
    def __call__(cls, key, *args, **kwargs):
        single_key = (cls, key)
        if single_key not in cls._instances:
            if key is not None:
                cls._instances[single_key] = super(KeyedSingleton, cls).__call__(
                key, *args, **kwargs)
            else:
                cls._instances[single_key] = super(KeyedSingleton, cls).__call__(
                    *args, **kwargs)
        return cls._instances[single_key]

    def _clear_singletons(self):
        '''For unittests only.'''
        self._instances = {}

class Singleton(KeyedSingleton):
    '''A Singleton metaclass so that only one version of the object type exists.

    To use, set the __metaclass__ property of your class. e.g.

    class MyClass(BaseClass):
        __metaclass__ = utils.obj.Singleton

    Then, every time you call MyClass(), you get the same object
    '''
    def __call__(cls, *args, **kwargs):
        return super(Singleton, cls).__call__(None, *args, **kwargs)



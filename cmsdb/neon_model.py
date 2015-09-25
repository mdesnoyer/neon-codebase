import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import datetime
from utils.options import define, options
from tornado import gen, ioloop, web
import psycopg2
import queries 

define("db_address", default="localhost", type=str, help="postgresql database address")
define("db_user", default="kevin", type=str, help="postgresql database user")
define("db_port", default=5432, type=int, help="postgresql port")
define("db_name", default="cmsdb", type=int, help="postgresql database name")

class DBConnection(web.RequestHandler): 
    def __init__(self):
        host = options.get('cmsdb.neon_model.db_address')
        port = options.get('cmsdb.neon_model.db_port')
        name = options.get('cmsdb.neon_model.db_name')
        user = options.get('cmsdb.neon_model.db_user')
        import pdb; pdb.set_trace()
        self.conn = queries.TornadoSession("postgresql://%s@%s:%s/%s" %  (user, host, port, name))

class StoredObject(object): 
    def __init__(self, key): 
        self.key = str(key)
        self.created = self.updated = str(datetime.datetime.utcnow()) 
          
    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return str(self)

    def __cmp__(self, other):
        classcmp = cmp(self.__class__, other.__class__) 
        if classcmp:
            return classcmp

        obj_one = set(self.__dict__).difference(('created', 'updated'))
        obj_two = set(other.__dict__).difference(('created', 'updated')) 
        classcmp = obj_one == obj_two and all(self.__dict__[k] == other.__dict__[k] for k in obj_one)

        if classcmp: 
            return 0
 
        return cmp(self.__dict__, other.__dict__)

    def save(self, callback=None):
        db_connection = DBConnection() 
        
    #def modify(cls, key, func, create_missing=False, callback=None):

class NamespacedStoredObject(StoredObject):
    '''An abstract StoredObject that is namespaced by the baseclass classname.

    Subclasses of this must define _baseclass_name in the base class
    of the hierarchy. 
    '''
    
    def __init__(self, key):
        super(NamespacedStoredObject, self).__init__(
            self.__class__.format_key(key))

    def get_id(self):
        '''Return the non-namespaced id for the object.'''
        return self.key2id(self.key)

    @classmethod
    def key2id(cls, key):
        '''Converts a key to an id'''
        return re.sub(cls._baseclass_name().lower() + '_', '', key)

    @classmethod
    def _baseclass_name(cls):
        '''Returns the class name of the base class of the hierarchy.

        This should be implemented in the base class as:
        return <Class>.__name__
        '''
        raise NotImplementedError()

    def _set_keyname(self):
        return 'objset:%s' % self._baseclass_name()

#class NeonUserAccount(NamespacedStoredObject):
#    def __init__(self, 
#                 a_id) 

 

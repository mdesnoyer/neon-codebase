import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from . import net

class PostgreSQLServer: 
   def __init__(self, port=None): 
       self.port = port 
        if self.port is None:
            self.port = net.find_free_port()


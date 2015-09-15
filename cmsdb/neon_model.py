import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import psycopg2
import queries 

define("db_address", default="localhost", type=str, help="")
define("db_port", default=6545, type=int, help="postgresql port")

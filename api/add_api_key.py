#use: ./script customer_name

#!/usr/bin/python
import tornado.escape
import sys
import hashlib

name = sys.argv[1]
fname = "apikeys.json"
key = hashlib.md5(name).hexdigest()

with open(fname, 'r') as f:
    json_data = f.readline()

data = tornado.escape.json_decode(json_data) 
data[name] = key
json = tornado.escape.json_encode(data)

with open(fname,'w') as f:
    f.write(json)


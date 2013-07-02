#!/usr/bin/python

''' Create api requests for the brightcove customers '''

from brightcove_api import BrightcoveApi
import tornado.escape

customer_file = 'brightcoveCustomerTokens.json'
customer_data = None

with open(customer_file,'r') as f:
    data = f.readline()

customer_data = tornado.escape.json_decode(data)

for customer_id in customer_data.keys():
    
    customer = customer_data[customer_id]
    rtoken = customer['read_token']
    wtoken = customer['write_token']
    napi   = customer['neon_api_key']
    pid   = customer['publisher_id']

    print "Retreiveing feed for " , customer_id

    bc = BrightcoveApi(publisher_id=pid, neon_api_key=napi, read_token=rtoken, write_token=wtoken)
    #bc.create_neon_api_requests(request_type='abtest')
    bc.create_neon_api_requests()

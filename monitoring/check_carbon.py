#!/usr/bin/env python
 
'''
Usage:
    graphite_check <check>

Script to query the monitoring server and check monitoring variables and 
their thresholds
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0, base_path)

from docopt import docopt
import inspect
import platform
import requests
from utils.options import options, define

define("graphite_server", default="http://54.225.235.97:8080", 
        help="monitoring server url", type=str)
define("monitoring_conf", default="config/neon-monitoring.conf",
        help="monitoring config file with thresholds for services", type=int)
 
GRAPHITE_SRV = 'http://54.225.235.97:8080'
MONITORING_CONF = 'config/neon-monitoring.conf'

def get_datapoints(json, target):
    return [d['datapoints'] for d in json if d['target'] == target][0]
 
def get_graphite_stats(target, from_str='-5min', to_str='now'):
    params = {'format': 'json',
              'target': target,
              'from': from_str,
              'to': to_str}
    response = requests.get('%s/render' % GRAPHITE_SRV, 
                            params=params, verify=False)
    response.raise_for_status()
    return response.json()

def get_program_thresholds(program):
    '''
    returns map server => [(program => thresholds) ..] 
    '''
    pass

def check_all():
    """
    Query Carbon server for current values
    """
    #Get all params
    with open(base_path + "/" +  MONITORING_CONF , "r") as stream:
        params = options._parse_config_file(stream)
        servers = params['servers']
        alert_hirearchy = params['system']
        for server, alerts in servers.iteritems():
            server_alerts = alerts['alerts'].split(',')
            for server_alert in server_alerts:
                #the service to monitor and its thresholds
                alert_threshold = alert_hirearchy[server_alert]
                for program, monitoring_vars in alert_threshold.iteritems():
                    for monitoring_var, threshold in monitoring_vars.iteritems():
                        service = "system.%s.%s.%s.%s" \
                                %(server, server_alert, program, monitoring_var)
                        json = get_graphite_stats('%s,"5min","avg",true)'%service)
                        value = json[0]['datapoints'][-1][0]
                        if float(value) > float(threshold):
                            print >> sys.stderr, \
                                    "service %s exceeds threshold: %s"\
                                    " current value: %s"%(service, value, threshold) 
                            yield 1 
                        else:
                            yield 0

def check_module(module, program, m_var):
    ''' Generic method to check  
    '''
    with open(base_path + "/" +  options.monitoring_conf, "r") as stream:
        params = options._parse_config_file(stream)
        threshold = params['system'][module][program][m_var]
        #servers = params['servers']
        #server = servers.keys()[0]
        server = platform.node().replace('.', '-')
        ret_val = 0 
        for server in servers.keys():
            service = "system.%s.%s.%s.%s" %(server, module, program, m_var) 
            json = get_graphite_stats('%s,"5min","avg",true)'%service)
            value = json[0]['datapoints'][-1][0]
            if value is None or (float(value) > float(threshold)):
                print >> sys.stderr, \
                    "service %s exceeds threshold: %s"\
                    " current value: %s"%(service, value, threshold)
    sys.exit(ret_val)

def check_services_internal_error():
    '''
    Check for internal errors on services servers
    '''
    module = 'supportServices'
    program = 'services'
    m_var = 'internal_err'
    check_module(module, program, m_var)

def check_services_bad_gateway():
    '''
    Check for bad gateway errors
    '''
    module = 'supportServices'
    program = 'services'
    m_var = 'bad_gateway'
    check_module(module, program, m_var)

def check_services_bad_request():
    '''
    Check for bad request errors
    '''
    module = 'supportServices'
    program = 'services'
    m_var = 'bad_request'
    check_module(module, program, m_var)

def main():
    ''' main '''

    local_functions = inspect.getmembers(sys.modules[__name__])
    checks = [func[0].replace('check_', '') for func in local_functions if func[0].startswith('check_')]
    doc = __doc__.replace('<check>', "(%s)" % " | ".join(checks))
    arguments = docopt(doc, version='1.0')
 
    check_name = [k for k, v in arguments.iteritems() if v][0]
    check_func = getattr(sys.modules[__name__], "check_%s" % check_name, False)
    print  check_func()
    #Alert if any service exceeds threshold
    #ret_vals = list(check_carbon())
    #if sum(ret_vals) >0:
    #    sys.exit(1)

if __name__ == '__main__':
    main()

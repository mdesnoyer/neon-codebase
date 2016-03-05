'''
Utilities to find prod machines


Author: Mark Desnoyer (desnoyer@neon-lab.com)
Date: Nov 2015
Copyright 2015 Neon Labs Inc.
'''
import boto.opsworks
import boto.rds

import logging
_log = logging.getLogger(__name__)

def find_host_private_address(hostname, stack_name=None,
                              aws_region='us-east-1'):
    '''Finds the private address of a host by name

    Inputs:
    hostname - Host name to search for
    stack_name - Optional stack name to limit search on
    aws_region - The region to search in
    '''
    conn = boto.opsworks.connect_to_region(aws_region)

    _log.info('Finding the ip address for %s' % hostname)
    
    # Find the stacks
    stack_ids = []
    for stack in conn.describe_stacks()['Stacks']:
        if stack_name is None or stack['Name'] == stack_name:
            stack_ids.append(stack['StackId'])
            break
    if len(stack_ids) == 0:
        raise ValueError('Could not find a valid stack %s' % stack_name)

    # Find the instance ip
    ip = None
    for stack_id in stack_ids:
        for instance in conn.describe_instances(
                stack_id=stack_id)['Instances']:
            try:
                if instance['Hostname'] == hostname:
                    ip = instance['PrivateIp']
                    _log.info('Found %s at %s' % (hostname, ip))
                    return ip
            except KeyError:
                pass
    raise ValueError('Could not find host %s' % hostname)

def find_rds_host(id):
    '''Returns the (host, port) of the rds instance referenced by id.'''
    conn = boto.rds.Connection()

    for instance in conn.get_all_dbinstances():
        if instance.id == id:
            return instance.endpoint

    raise ValueError('Could not find RDS database %s' % id)

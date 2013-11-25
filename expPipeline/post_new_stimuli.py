#!/usr/bin/env python
'''
Script that watches looks at a list of stimuli sets and if there are new ones,
posts them to mechanical turk.

Assumes that the stimuli sets will appear in directories named
"stimuli_#" and that a file called stimuli.posted will be in the
stimuli directory that contains a single line equal to the number of
the last stimuli set posted.

Copyright: 2013 Neon Labs
Author: Sunil Mallya (mallaya@neon-lab.com)
        Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import logging
from optparse import OptionParser
import os
import psycopg2 as db
import re
import subprocess
import sys
import utils.logs

_log = logging.getLogger(__name__)

_status_file = 'stimuli.posted'

_PROD_APP = 'gentle-escarpment-8454'
_SANDBOX_APP = 'gentle-escarpment-8454-staging'

def get_last_posted(db_connection):
    cursor = db_connection.cursor()
    stimsetRe = re.compile('stimuli_([0-9]+)')

    cursor.execute('select distinct stimset_id from image_choices')
    latest = 0
    for stimset_id in cursor:
        match = stimsetRe.search(stimset_id[0])
        if match:
            cur_count = int(match.groups()[0])
            if cur_count > latest:
                latest = cur_count

    return latest

def update_last_posted(directory, new_id):
    '''Writes the last posted id to file'''
    with open(os.path.join(directory, _status_file), 'wt') as f:
        f.write('%i' % new_id)

def get_new_sets(stimuli_dir, last_posted, max_hits=None):

    should_post = lambda cur_id: (cur_id > last_posted and
                                  (max_hits is None or 
                                   cur_id <= (last_posted + max_hits)))
    to_post = []
    max_id = last_posted
    stimsetRe = re.compile('stimuli_([0-9]+)')
    for filename in os.listdir(stimuli_dir):
        if (os.path.isdir(os.path.join(stimuli_dir, filename)) and 
            (stimsetRe.match(filename) is not None)):
            cur_id = int(stimsetRe.match(filename).groups()[0])
            if should_post(cur_id):
                to_post.append(os.path.join(stimuli_dir, filename))
                _log.info('Found new stimuli set: %s' % filename)
                if cur_id > max_id:
                    max_id = cur_id

    return (to_post, max_id)

def post_new_sets(mturk_dir, sets, pay, assignments, sandbox):
    _log.info('Posting %i new sets' % len(sets))

    sandboxStr = ''
    if sandbox:
        sandboxStr = '--sandbox'
    
    cmd = '%s  %s --pay %f --assignments %i -i %s' % (
        os.path.join(mturk_dir, 'post_hits.rb'),
        sandboxStr,
        pay,
        assignments,
        ' '.join(sets))

    try:
        subprocess.check_call(cmd, cwd=mturk_dir, shell=True)
    except subprocess.CalledProcessError as e:
        _log.exception('Error posting new sets: %s' % e)
        raise

def dynos_running(sandbox):
    '''Returns the number of dynos currently running.'''
    app = _PROD_APP
    if sandbox:
        app = _SANDBOX_APP

    output = subprocess.check_output('heroku ps --app %s' % app, shell=True)

    dynoRe = re.compile('web\.[0-9]+: up')
    dynos_found = 0
    for line in output.split('\n'):
        if dynoRe.search(line):
            dynos_found += 1

    return dynos_found

def main(options):
    if ((options.max_dynos_allowed is not None) and
        (dynos_running(options.sandbox) >= options.max_dynos_allowed)):
        _log.info('Server is too busy. Not posting new jobs.')
        return

    
    db_connection = db.connect(database = options.database,
                               user = options.db_user,
                               password = options.db_pass,
                               host = options.db_host,
                               port = options.db_port)
    try:
        last_posted = get_last_posted(db_connection)
    finally:
        db_connection.close()

    to_post, max_id = get_new_sets(options.stimuli_dir, last_posted,
                                   options.max_hits)

    if len(to_post) > 0:
        post_new_sets(options.mturk_dir,
                      to_post,
                      options.pay,
                      options.assignments,
                      options.sandbox)

    update_last_posted(options.stimuli_dir, max_id)

if __name__ == '__main__':
    parser = OptionParser()
    
    parser.add_option('--stimuli_dir', default=None,
                      help='Directory containing the stimuli sets.')
    parser.add_option('--mturk_dir', default=None,
                      help='Directory containing the mechanical turk app')
    parser.add_option('--pay', default=1.00, type='float',
                      help='Amount to pay Turkers')
    parser.add_option('--assignments', default=30, type='int',
                      help='Number of assingments for each job')
    parser.add_option('--max_hits', default=None, type='int',
                      help='Maximum number of hits to post at once')
    parser.add_option('--max_dynos_allowed', default=None, type='int',
                      help='If set, new jobs will not be added unless there are less dynos currently running compared to this setting.')
    parser.add_option('--sandbox', default=False, action='store_true',
                      help='If set, uses the sandbox settings')
    parser.add_option('--log', default=None,
                      help='Log file. If none, dumps to stdout')

    # Database options
    parser.add_option('--db_host',
                      default='ec2-23-23-234-207.compute-1.amazonaws.com',
                      help='Database host')
    parser.add_option('--database',
                      default='d818kso4dkedro',
                      help='Database to connect to')
    parser.add_option('--db_user', default='ypqkdxvnyynxtc',
                      help='Database user')
    parser.add_option('--db_port', default=5432, type='int',
                      help='Database port')
    parser.add_option('--db_pass', default='Kr3_R6gYvLqGk_WnYc9dclK6sF',
                      help='Database password')

    options, args = parser.parse_args()

    if options.log is None:
        utils.logs.StreamLogger(None)
    else:
        utils.logs.FileLogger(None, options.log)

    main(options)

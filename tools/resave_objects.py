#!/usr/bin/env python
'''
Script that resaves a bunch of objects in order to migrate the database.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__),  '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import logging
import time
import tornado.ioloop
import utils.http
import utils.neon

_log = logging.getLogger(__name__)

def main():
    shrunken_count = {}
    same_count = {}
    n_processed = 0

    for thumb_urls in neondata.ThumbnailServingURLs.iterate_all():
        account_id = thumb_urls.get_id().partition('_')[0]
        if len(thumb_urls.size_map) == 0:
            shrunken_count[account_id] = shrunken_count.get(account_id, 0) + 1
        else:
            same_count[account_id] = same_count.get(account_id, 0) + 1

        try:
            thumb_urls.save()
        except Exception as e:
            _log.error('Error saving thumbnail %s' % thumb_urls.get_id())

        n_processed += 1
        if n_processed % 1000 == 0:
            _log.info('Processed %i thumbs' % n_processed)

    for acct, count in shrunken_count.iteritems():
        _log.info('Account %s has %i thumbs that have been shrunken' % 
                  (acct, count))

    for acct, count in same_count.iteritems():
        _log.warn('Account %s has %i thumbs that have not been shrunken' % 
                  (acct, count))


if __name__ == '__main__':
    utils.neon.InitNeon()
    main()

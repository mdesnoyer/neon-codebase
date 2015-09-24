#!/usr/bin/env python
'''
Script that fixes the quebecor serving urls

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import logging
import re
import tornado.ioloop
import utils.neon


_log = logging.getLogger(__name__)

@tornado.gen.coroutine
def main():
    API_KEY = 'fnituxjx79v5bfahihppceqq'
    cdn_info = neondata.CDNHostingMetadataList.get(
        '%s_69' % API_KEY)
    urlRe = re.compile('(http://static.neon.groupetva.ca)/(fnituxjx79v5bfahihppceqq.*)')
    needed_renditions = set([tuple(x) 
                             for x in cdn_info.cdns[0].rendition_sizes])

    to_save = []
    n_mod = 0
    n_still_bad = 0
    count = 0
    for urls in neondata.ThumbnailServingURLs.iterate_all():
        if not urls.get_id().startswith(API_KEY):
            continue

        url_match = urlRe.match(urls.base_url)
        if not url_match:
            continue
        
        if count % 1000 == 0:
            _log.info('Processed %d thumbs' % count)
        count += 1
            

        # Change the base url
        urls.base_url = '%s/%s/%s' % (url_match.group(1),
                                     cdn_info.cdns[0].folder_prefix,
                                     url_match.group(2))
        to_save.append(urls)

        if needed_renditions == urls.sizes:
            urls.size_map = {}
            n_mod += 1

        else:
            _log.warning('Thumbnail %s is still bad' % urls.get_id())
            n_still_bad += 1

        if len(to_save) >= 20:
            neondata.ThumbnailServingURLs.save_all(to_save)
            to_save = []

    neondata.ThumbnailServingURLs.save_all(to_save)
    _log.info('Processed %d thumbs. %d modified. %d still bad' %
              (count, n_mod, n_still_bad))

if __name__ == '__main__':
    utils.neon.InitNeon()
    tornado.ioloop.IOLoop.current().run_sync(main)

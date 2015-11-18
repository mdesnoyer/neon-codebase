#!/usr/bin/env python
'''
Replicates the function of make_pip_repo, but checks that every link that
points to a S3 resource is valid (i.e., is not a dead link)

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)

Adapted by:
Nick Dufour 2015
'''
USAGE='%prog [options] <package1> <package2> etc..'
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import boto.s3.acl
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import logging
import re
import utils.neon
from utils.options import define, options

import urllib
from HTMLParser import HTMLParser

class MyHTMLParser(HTMLParser):

    lasttag = None
    attr = None

    def handle_starttag(self, tag, attr):
        self.lasttag = tag.lower()
        if len(attr):
            if attr[0][0] == 'href':
                self.attr = attr[0][1]

def validate_index(index):
    invalid = []
    valid = []
    for i in index.split('\n'):
        parser = MyHTMLParser()
        parser.feed(i)
        if parser.attr is not None:
            if 'http' == parser.attr[:4]:
                z = urllib.urlopen(parser.attr)
                if z.code != 404:
                    valid.append(i)
                else:
                    print 'Invalid: %s'%(i)
                    invalid.append(i)
            else:
                valid.append(i)
        else:
            valid.append(i)
    return '\n'.join(valid)

define('s3bucket', default='neon-dependencies',
       help='Bucket in s3 to put the dependencies in')
define('append', default=1, type=int,
       help='If 1, will append the files to the repo. Otherwise overwrites.')


_log = logging.getLogger(__name__)

#pkgRe = re.compile('([a-zA-Z0-9_\-\.]+)\.(tar\.gz|zip|tar\.bz2|whl)')
pkgRe = re.compile('([a-zA-Z0-9_\-\.]+)\.(tar\.gz|zip|tar\.bz2|whl|tgz)')

def build_index():
    '''Creates an html file that will point to the packages stored there.'''
        
    # First get the existing index
    new_index = ''
    if options.append:
        s3conn = S3Connection()
        bucket = s3conn.get_bucket(options.s3bucket)
        key = bucket.get_key('index.html')
        if key is not None:
            new_index = key.get_contents_as_string()

    # Parse the packages to upload and create the links
    for pkg_fn in sys.argv[1:]:
        pkg_match = pkgRe.search(pkg_fn)
        if pkg_match:
            pkg_name = pkg_match.group(1)
            new_index += (
                '<a href="http://s3-us-west-1.amazonaws.com/%s/%s">%s</a><br>\n' %
                (options.s3bucket, os.path.basename(pkg_fn), pkg_name))
    new_index = validate_index(new_index)
    return new_index
                          
def main():
    if len(sys.argv) < 2:
        _log.fatal(USAGE)
        exit(1)
        
    s3conn = S3Connection()
    bucket = s3conn.get_bucket(options.s3bucket)

    # Upload all the packages
    for pkg_fn in sys.argv[1:]:
        pkg_match = pkgRe.search(pkg_fn)
        if pkg_match:
            _log.info('Uploading %s' % pkg_fn)
            key = Key(bucket, os.path.basename(pkg_fn))
            if pkg_match.group(2) == 'zip':
                mime = 'application/zip'
            elif pkg_match.group(2) == 'tar.gz':
                mime = 'application/x-gzip'
            elif pkg_match.group(2) == 'tgz':
                mime = 'application/x-gzip'
            elif pkg_match.group(2) == 'tar.bz2':
                mime = 'application/x-bzip2'
            elif pkg_match.group(2) == 'whl':
                mime = 'application/whl'
            
            key.set_contents_from_filename(
                pkg_fn,
                headers={'Content-Type' : mime},
                replace=False)
            key.set_acl('public-read')

    # Now upload the index
    key = Key(bucket, 'index.html')
    key.set_contents_from_string(
        build_index(),
        headers={'Content-Type' : 'text/html'}
        )
    key.set_acl('public-read')

if __name__ == '__main__':
    utils.neon.InitNeon(usage=USAGE)
    main()

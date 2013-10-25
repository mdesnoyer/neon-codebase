'''
A module to wrap options at the command line or in a config file.

A module using this functionality would define its own options like:

  from utils.options import define, options

  define("mysql_host", default="127.0.0.1:3306", type=str,
         help="Main user DB")

  def connect():
    db = database.Connection(options.mysql_host)

Then, in the main() routine the parsing command must be called:

  utils.options.parse_options()

Options can be defined in the command line or in a configuration
file. If you want the configuration to load from the configuration
file, simply add the "--config" option to the command line. Command
line options take precendence over the config file. e.g.

  ./my_executable --config /path/to/config/file

Paths can either be a local path, or to S3 in the form:

   s3://bucket/path/to/file

If you are using s3, the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
environment variables must be set.

If you are using a config file, it will automatically be polled for
changes. So, to push new parameter values, you just need to update the
file that is specified by the --config variable

All of the options are namespaced by module name. So, if the above was
in the module mastermind.core, the option at the command line would
be:

  --mastermind.core.mysql_host 127.0.0.1

While the config file is in yaml format and would look like:

  mastermind:
    core:
      mysql_host: 127.0.0.1
      mysql_port: 9854

For variables defined in __main__, they are at the root of the option hierarchy

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs

Modelled after the tornado options module with a few differences.

'''
import boto
import boto.utils
import inspect
import logging
import optparse
import os.path
import re
import sys
import threading
import yaml

#TODO(mdesnoyer): Add support for booleans


_log = logging.getLogger(__name__)

class Error(Exception):
    """Exception raised by errors in the options module."""
    pass

class OptionParser(object):
    '''A collection of options.'''
    def __init__(self):
        self.__dict__['_options'] = {}
        self.__dict__['lock'] = threading.RLock()
        self.__dict__['cmd_options'] = None
        self.__dict__['last_update'] = None

        # Find the root directory of the source tree
        cur_dir = os.path.abspath(os.path.dirname(__file__))
        while cur_dir <> '/':
            if os.path.exists(os.path.join(cur_dir, 'NEON_ROOT')):
                self.__dict__['NEON_ROOT'] = cur_dir
                break
            cur_dir = os.path.abspath(os.path.join(cur_dir, '..'))

        if cur_dir == '/':
            raise Error('Could not find the NEON_ROOT file in the source tree.')

    def __getattr__(self, name):
        with self.__dict__['lock']:
            global_name = self._local2global(name)
            if isinstance(self._options.get(global_name), _Option):
                return self._options[global_name].value()
            raise AttributeError("Unrecognized option %r" % global_name)

    def __setattr__(self, name, value):
        raise NotImplementedError(
            'Sorry, you cannot set parameters at runtime.')

    def __getitem__(self, name):
        with self.__dict__['lock']:
            global_name = self._local2global(name)
            return self._options[global_name].value()

    def define(self, name, default=None, type=None, help=None, stack_depth=2):
        '''Defines a new option.

        Inputs:
        name - The name of the option
        default - The default value
        type - The type object to expect
        help - Help string
        '''
        global_name = self._local2global(name, stack_depth=stack_depth)

        if global_name in self._options:
            raise Error("Option %s already defined." % global_name)

        if type == bool:
            raise TypeError('Boolean variables are not supported. Variable: %s'
                            % global_name)
        if isinstance(type, basestring):
            raise TypeError('Type must be specified uisng the python type not '
                            'a string. Variable: %s' % global_name)

        if type is None:
            if default is None:
                type = str
            else:
                type = default.__class__

        self._options[global_name] = _Option(name, default=default,
                                             type=type, help=help)

    def parse_options(self, args=None, config_stream=None,
                      usage='%prog [options]'):
        '''Parse the options.

        Inputs:
        args - Argument list. defaults to sys.argv[1:]
        config_stream - Specify a yaml stream to get arguments from. 
                        Otherwise looks for the --config flag in the arguments.
        usage - To display the usage.
        '''
        with self.__dict__['lock']:
            cmd_options, args = self._parse_command_line(args, usage)

            self._register_command_line_options()

            # Now, process the configuration file if it exists
            self._process_new_config_file(config_stream,
                                          self.cmd_options.config)

            # Start the polling thread if there is a config file to read
            if self.cmd_options.config is not None:
                ConfigPoller(self).start()

        return self, args

    def _parse_command_line(self, args=None, usage='%prog [options]'):
        '''Parse the command line.'''
        if args is None:
            args = sys.argv[1:]

        # First parse the command line
        cmd_parser = optparse.OptionParser(usage=usage)

        cmd_parser.add_option('--config', '-c', default=None,
                              help='Path to the config file')

        groups = {}
        groupRe = re.compile('(.+)\\.[a-zA-Z0-9-_]+$')

        for name, option in sorted(self._options.items()):
            # We group by the module name to make the help message
            # easier to read.
            groupMatch = groupRe.match(name)
            if groupMatch:
                group_name = groupMatch.groups()[0]
                group = groups.setdefault(group_name, optparse.OptionGroup(
                    cmd_parser, group_name))
            else:
                group = cmd_parser
            group.add_option('--%s' % name,
                             default=None,
                             type=option.type.__name__,
                             help=option.help)

        for group in groups.itervalues():
            cmd_parser.add_option_group(group)

        self.__dict__['cmd_options'], args = cmd_parser.parse_args(args)

        return self.cmd_options, args

    def _process_new_config_file(self, stream=None, path=None):
        '''Deals with a config file if present and new.'''
        yaml_parse = self._parse_config_file(stream,
                                             self.cmd_options.config)

        if yaml_parse is not None:
            with self.__dict__['lock']:
                self._reset_options()
                self._register_command_line_options()
                self._parse_dict(yaml_parse, '')

    def _reset_options(self):
        '''Resets all the options with their default values.'''
        for name, obj in self._options.iteritems():
            obj.reset()

    def _register_command_line_options(self):
        '''Takes the options in self.cmd_options and registers them.'''
        for name, value in self.cmd_options.__dict__.iteritems():
            if name == 'config':
                continue
            if value is not None:
                self._options[name].set(value)


    def _parse_config_file(self, stream=None, path=None):
        '''Parses the yaml config file if it is new.

        Inputs:
        stream - Stream with the yaml data
        path - If there is no stream, try to find the config file at this path

        Outputs:
        A dictionary of the yaml parsing or None if there was no new data
        '''
        if stream is not None:
            return yaml.load(stream)        

        s3re = re.compile('s3://([0-9a-zA-Z\.\-]+)/([0-9a-zA-Z\.\-/]+)')

        if path is not None:
            s3match = s3re.match(path)
            if s3match:
                # Handle reading from S3
                bucket_name, key_name = s3match.groups()
                s3conn = boto.connect_s3()
                bucket = s3conn.get_bucket(bucket_name)
                key = bucket.get_key(key_name)
                if key is None:
                    raise KeyError('Could not find key %s in S3 bucket %s' %
                                   (key_name, bucket_name))

                # See if the key is new
                mod_time = boto.utils.parse_ts(key.last_modified)
                if self.last_update is None or self.last_update < mod_time:
                    with self.__dict__['lock']:
                        self.__dict__['last_update'] = mod_time
                    _log.info(
                        'Reading new config file from S3 bucket: %s key: %s' %
                        (bucket_name, key_name))
                    with key.open() as f:
                        return yaml.load(f)

            else:
                # Try opening the config file locally
                mod_time = os.path.getmtime(path)
                if self.last_update is None or self.last_update < mod_time:
                    _log.info('Reading new local config file %s' % path)
                    with self.__dict__['lock']:
                        self.__dict__['last_update'] = mod_time
                    with open(path) as f:
                        return yaml.load(f)

        return None       

    def _parse_dict(self, d, prefix):
        '''Parses a nested dictionary and stores the variables values.'''
        for key, value in d.iteritems():
            if prefix == '':
                name = key
            else:
                name = '%s.%s' % (prefix, key)
            if type(value) == dict:
                self._parse_dict(value, name)
                continue

            try:
                option = self._options[name]
                if option._value is None:
                    option.set(option.type(value))
            except KeyError:
                raise AttributeError('Unknown option %s' % name)
            except ValueError:
                raise TypeError('For option %s could not convert "%s" to %s' %
                                (name, value, option.type.__name__))

    def _local2global(self, option, stack_depth=2):
        '''Converts the local name of the option to a global one.

        e.g. if define("font", ...) is in utils, this returns "utils.font"

        Stack depth controls how far back the module is found.
        Normally this is 2.
        '''
        frame = inspect.stack()[stack_depth]
        mod = inspect.getmodule(frame[0])
        
        if mod.__name__ in ['__main__', '', '.', None]:
            return option

        modpath = os.path.abspath(mod.__file__)
        relpath = os.path.relpath(modpath, self.NEON_ROOT)
        relpath = os.path.splitext(relpath)[0]
        relmod = '.'.join(relpath.split('/'))
        return '%s.%s' % (relmod, option)
        

class _Option(object):
    def __init__(self, name, default=None, type=str, help=None):
        self.name = name
        self.default = default
        self.type = type
        self.help = help
        self._value = None

    def value(self):
        return self.default if self._value is None else self._value

    def set(self, value):
        self._value = value

    def reset(self):
        self._value = None

class ConfigPoller(threading.Thread):
    '''A thread that polls for updates in the config file.'''
    def __init__(self, parser):
        super(ConfigPoller, self).__init__()
        self.parser = parser
        self.daemon = True

    def run(self):
        while True:
            try:
                if self.parser.cmd_options is not None:
                    self.parser._process_new_config_file(
                        path = self.parser.cmd_options.config)
            except Exception as e:
                _log.exception('Error processing config file: %s' % e)

            # Don't poll the file too much
            threading.Event().wait(30)


options = OptionParser()
'''Global options object.'''

def define(name, default=None, type=None, help=None):
    return options.define(name, default=default, type=type, help=help,
                          stack_depth=3)


def parse_options(args=None, config_stream=None,
                  usage='%prog [options]'):
    return options.parse_options(args=args, config_stream=config_stream,
                                 usage=usage)

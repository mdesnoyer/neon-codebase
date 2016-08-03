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

For variables defined in __main__, they are at the root of the option
hierarchy although they can be specified in the config file in their
modules instead.

If you are forking new processes in your code, the subprocesses will
only be able to access those options that are available when the
process forks. In practice, if you are using global options and fork
after all the importing is done, you will be fine.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs

Modelled after the tornado options module with a few differences.

'''
import boto
import boto.utils
import contextlib
import inspect
import logging
import multiprocessing
import optparse
import os.path
import re
import sys
import threading
import time
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
        self.__dict__['lock'] = multiprocessing.RLock()
        self.__dict__['cmd_options'] = None
        self.__dict__['last_update'] = None
        self.__dict__['_config_poll_timer'] = None

        # Find the root directory of the source tree
        cur_dir = os.path.abspath(os.path.dirname(__file__))
        while cur_dir <> '/':
            if os.path.exists(os.path.join(cur_dir, 'NEON_ROOT')):
                self.__dict__['NEON_ROOT'] = cur_dir
                break
            cur_dir = os.path.abspath(os.path.join(cur_dir, '..'))

        if cur_dir == '/':
            raise Error('Could not find the NEON_ROOT file in the source tree.')

        # Find the full prefix for the main module
        self.__dict__['main_prefix'] = self._get_main_prefix()

    def __del__(self):
        timer = self.__dict__['_config_poll_timer']
        if timer is not None and timer.is_alive():
            timer.cancel()
        self.__dict__['_config_poll_timer'] = None
        

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

    def define(self, name, default=None, type=None, help=None, stack_depth=2,
               max_str_size=256):
        '''Defines a new option.

        Inputs:
        name - The name of the option
        default - The default value
        type - The type object to expect
        help - Help string
        stack_depth - Frame stack depth where the option was defined
        max_str_size - Maximum size of a string option
        '''
        global_name = self._local2global(name, stack_depth=stack_depth)

        if global_name in self._options:
            known_option = self._options[global_name]
            if (known_option.default == default and 
                (type is None or known_option.type == type) and
                known_option.help == help):
                # It's redefined exactly the same. Ignore silently.
                return
            else:
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
                                             type=type, help=help,
                                             max_size=max_str_size)

    def parse_options(self, args=None, config_stream=None,
                      usage='%prog [options]', watch_file=True):
        '''Parse the options.

        Inputs:
        args - Argument list. defaults to sys.argv[1:]
        config_stream - Specify a yaml stream to get arguments from. 
                        Otherwise looks for the --config flag in the arguments.
        usage - To display the usage.
        watch_file - If true, the config file will be watched for changes 
                     that will be incorporated on the fly.
        '''
        with self.__dict__['lock']:
            cmd_options, args = self._parse_command_line(args, usage)

            self._register_command_line_options()

            # Now, process the configuration file if it exists
            self._process_new_config_file(config_stream,
                                          self.cmd_options.config)

            # Start the polling thread if there is a config file to read
            if watch_file and self.cmd_options.config is not None:
                if self.__dict__['_config_poll_timer'] is not None:
                    self.__dict__['_config_poll_timer'].cancel()
                    self.__dict__['_config_poll_timer'] = None
                self._poll_config_file()

        return self, args

    def _poll_config_file(self):
        '''Polls the config file to see if it has changed.'''
        try:
            if self.cmd_options is not None:
                self._process_new_config_file(
                    path = self.cmd_options.config)
        except Exception as e:
            _log.exception('Error processing config file %s: %s' % 
                           (self.cmd_options.config, e))

        # Schedule the next time to poll the config file
        self.__dict__['_config_poll_timer'] = threading.Timer(
            2.0, self._poll_config_file)
        self.__dict__['_config_poll_timer'].daemon = True
        self.__dict__['_config_poll_timer'].start()

    def get(self, global_name):
        '''Retrieve the value of the options with a given global name.

        This is mostly a helper for debugging since pdb breaks the
        local to global name conversion. In your code, you're better
        off just doing options.local_name

        The only other time you would want to call this is if you are
        in a tight loop and want to avoid the introspection, which is
        slow.
        
         ''' 
        with self.__dict__['lock']:
            return self._options[global_name].value()

    def get_config_file(self):
        '''Returns the config file name that is used by this parser.'''
        return self.cmd_options.config

    def options_loaded(self):
        '''Returns True if the options have been parsed and loaded.

        If False, you need to run parse_options
        '''
        return self.cmd_options is not None

    @contextlib.contextmanager
    def _set_bounded(self, global_name, value):
        '''Sets the value of an option in a bounded region.

        This should only be used in testing setups and lets you do:
        with options._set_bounded('my.var', 95):
          do_stuff()
        '''
        option = self._options[global_name]
        is_default = option.is_default()
        old_val = option.value()
        option.set(value)
        try:
            yield
        finally:
            if is_default:
                option.reset()
            else:
                option.set(old_val)

    def _set(self, global_name, value):
        '''Sets the value of an option.

        Outside of this module, this should only be used in test setups,
        primarily so that you can do:
        def setUp(self):
          self.old_variable = options.get('my.variable')
          options._set('my.variable', 45)

        def tearDown(self):
          options._set('my.variable', self.old_variable)
        '''
        with self.__dict__['lock']:
            try:
                option = self._options[global_name]
                option.set(value)
            except KeyError:
                _log.warn('Cannot set %s. It does not exist' % global_name)

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
                             metavar=name.split('.')[-1].upper(),
                             type=option.type.__name__,
                             help='%s [default: %s]' % (option.help,
                                                       option.default))

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
        with self.__dict__['lock']:
            for name in self._options.keys():
                obj = self._options[name]
                obj.reset()

    def _register_command_line_options(self):
        '''Takes the options in self.cmd_options and registers them.'''
        for name, value in self.cmd_options.__dict__.iteritems():
            if name == 'config':
                continue
            if value is not None:
                self._set(name, value)


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

        s3re = re.compile('s3://([0-9a-zA-Z\.\-_]+)/([0-9a-zA-Z\.\-/]+)')

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
            if prefix == '' or prefix == self.main_prefix:
                name = key
            else:
                name = '%s.%s' % (prefix, key)
            if type(value) == dict:
                self._parse_dict(value, name)
                continue

            try:
                option = self._options[name].set(value,
                                                 ignore_if_already_set=True)
            except KeyError:
                _log.warn('Unknown option %s. Ignored' % name)

    def _local2global(self, option, stack_depth=2):
        '''Converts the local name of the option to a global one.

        e.g. if define("font", ...) is in utils.py, this returns "utils.font"

        Stack depth controls how far back the module is found.
        Normally this is 2.
        '''
        frame = inspect.currentframe()
        for i in range(stack_depth):
            frame = frame.f_back
        mod = inspect.getmodule(frame)
        
        if (mod.__name__ in ['__main__', '', '.', None] or
            mod.__name__.endswith('options_test')):
            return option

        return '%s.%s' % (self._get_option_prefix(mod.__file__), option)

    def _get_main_prefix(self):
        for frame in inspect.stack():
            mod = inspect.getmodule(frame[0])
            if mod is None:
                return ''
            if re.search(r'airflow', mod.__name__):
                # running from within Airflow, so return an empty string
                # otherwise the result is '............usr.local.bin.airflow'
                return ''
            if (mod.__name__ == '__main__' or
                mod.__name__.endswith('options_test')):
                return self._get_option_prefix(mod.__file__)
        raise Error("Could not find the main module")

    def _get_option_prefix(self, filename):
        '''Returns the module-like prefix for a given filename.'''
        
        apath = os.path.abspath(filename)
        relpath = os.path.relpath(apath, self.NEON_ROOT)
        relpath = os.path.splitext(relpath)[0]
        return '.'.join(relpath.split('/'))

class _Option(object):
    # Enums to put for the status of data captured in the value
    _DATA_DEFAULT = 0
    _DATA_VALID_VAL = 1
    _DATA_NONE = 2
    
    
    def __init__(self, name, default=None, type=str, help=None, max_size=256):
        self.name = name
        self.default = default
        self.type = type
        self.help = help
        self.max_size = max_size
        self._create_mutable_fields(type, max_size)

    def value(self):
        '''Get the value of the option.'''
        with self._lock:
            status = self._data_status.value
            if status == _Option._DATA_DEFAULT:
                return self.default
            elif status == _Option._DATA_NONE:
                return None
            else:
                return self._value.value

    def set(self, value, ignore_if_already_set=False):
        '''Sets the value of this option.'''
        with self._lock:
            if (ignore_if_already_set and 
                self._data_status.value != _Option._DATA_DEFAULT):
                return
            if value is None:
                self._data_status.value = _Option._DATA_NONE
            else:
                if (issubclass(self.type, basestring) and 
                    len(value) >= self.max_size):
                    msg = ('String for option %s is too long (%d): %s' %
                        (self.name, self.max_size, value))
                    _log.error(msg)
                    raise ValueError(msg)
                try:
                    val = self.type(value)
                except ValueError:
                    raise TypeError(
                        'For option %s could not convert "%s" to %s' %
                        (self.name, value, self.type.__name__))
                self._value.value = val
                self._data_status.value = _Option._DATA_VALID_VAL

    def reset(self):
        self._data_status.value = _Option._DATA_DEFAULT

    def is_default(self):
        self._data_status.value == _Option._DATA_DEFAULT

    def _create_mutable_fields(self, type, max_size):
        self._lock = multiprocessing.RLock()
        if type == int:
            self._value = multiprocessing.Value('i', 0, lock=self._lock)
        elif type == long:
            self._value = multiprocessing.Value('l', 0L, lock=self._lock)
        elif type == float:
            self._value = multiprocessing.Value('d', 0.0, lock=self._lock)
        elif issubclass(type, basestring):
            self._value = multiprocessing.Array('c', max_size, lock=self._lock)
        else:
            raise ValueError('Type %s is not supported' % type)
        
        self._data_status = multiprocessing.Value('b', _Option._DATA_DEFAULT,
                                                  lock=self._lock)


options = OptionParser()
'''Global options object.'''

def define(name, default=None, type=None, help=None, max_str_size=256):
    return options.define(name, default=default, type=type, help=help,
                          stack_depth=3, max_str_size=max_str_size)


def parse_options(args=None, config_stream=None,
                  usage='%prog [options]', watch_file=True):
    return options.parse_options(args=args, config_stream=config_stream,
                                 usage=usage, watch_file=watch_file)

def options_loaded():
    return options.options_loaded()

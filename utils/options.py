'''
A module to wrap options at the command line or in a config file.

A module using this functionality would define its own options like:

  from utils.options import define, options

  define("mysql_host", default="127.0.0.1:3306", type=str,
         help="Main user DB")

  def connect():
    db = database.Connection(options.mysql_host)

Then, in the main() routine the parsing command must be called:

  utils.options.parse_args()

Options can be defined in the command line or in a configuration
file. If you want the configuration to load from the configuration
file, simply add the "--config" option to the command line. Command
line options take precendence over the config file. e.g.

  ./my_executable --config /path/to/config/file

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
import inspect
import optparse
import os.path
import sys
import yaml

class Error(Exception):
    """Exception raised by errors in the options module."""
    pass

class OptionParser(object):
    '''A collection of options.'''
    def __init__(self):
        self.__dict__['_options'] = {}

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
        global_name = self._local2global(name)
        if isinstance(self._options.get(global_name), _Option):
            return self._options[global_name].value()
        raise AttributeError("Unrecognized option %r" % global_name)

    def __setattr__(self, name, value):
        raise NotImplementedError('Sorry you cannot set parameters at runtime.''')

    def __getitem__(self, name):
        global_name = self._local2global(name)
        return self._options[global_name].value()

    def define(self, name, default=None, type=None, help=None):
        '''Defines a new option.

        Inputs:
        name - The name of the option
        default - The default value
        type - The type object to expect
        help - Help string
        '''
        global_name = self._local2global(name)

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
        if args is None:
            args = sys.argv[1:]
        
        # First parse the command line
        cmd_parser = optparse.OptionParser(usage=usage)

        cmd_parser.add_option('--config', '-c', default=None,
                              help='Path to the config file')

        for name, option in sorted(self._options.items()):
            cmd_parser.add_option('--%s' % name,
                                  default=None,
                                  type=option.type.__name__,
                                  help=option.help)

        cmd_options, args = cmd_parser.parse_args(args)

        for name, value in cmd_options.__dict__.iteritems():
            if name == 'config':
                continue
            if value is not None:
                self._options[name].set(value)

        # Now, parse the configuration file if it exists
        #TODO(mdesnoyer): Enable reading from an s3 source
        yaml_parse = None
        if config_stream is not None:
            yaml_parse = yaml.load(config_stream)

        elif cmd_options.config is not None:
            with open(cmd_options.config) as f:
                yaml_parse = yaml.load(f)

        if yaml_parse is not None:
            self._parse_dict(yaml_parse, '')

        return self, args

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
                raise TypeError('For option %s could not convert %s to %s' %
                                (name, value, option.type.__name_))

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


options = OptionParser()
'''Global options object.'''

def define(name, default=None, type=None, help=None):
    return options.define(name, default=default, type=type, help=help)


def parse_options(self, args=None, config_stream=None,
                   usage='%prog [options]'):
    return options.parse_options(args=args, config_stream=config_stream,
                                 usage=usage)

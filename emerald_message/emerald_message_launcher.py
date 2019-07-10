import argparse
import os
import platform
import sys
import datetime
import pkg_resources

from pytz import timezone

from emerald_message.exitcode import ExitCode
from emerald_message.logging.logger import EmeraldLogger
from emerald_message.version import __version__

MIN_PYTHON_VER_MAJOR = 3
MIN_PYTHON_VER_MINOR = 6


def get_command_info_as_string() -> str:
    the_package = __name__
    the_path = '/'.join(('reference', 'command_notes.txt'))
    the_reference_info = pkg_resources.resource_string(the_package, the_path).decode('utf-8')

    return the_reference_info


def emerald_message_launcher(argv):
    try:
        appname = __name__.split('.')[0]
    except KeyError:
        appname = __name__

    if sys.version_info.major < MIN_PYTHON_VER_MAJOR or (
            sys.version_info.major == MIN_PYTHON_VER_MAJOR and sys.version_info.minor < MIN_PYTHON_VER_MINOR):
        print('Unable to run this script - python interpreter must be at or above version ' +
              str(MIN_PYTHON_VER_MAJOR) + '.' + str(MIN_PYTHON_VER_MINOR) +
              os.linesep + '\tVersion is ' + str(sys.version_info.major) + '.' + str(sys.version_info.minor))
        return ExitCode.PythonVersionError
    else:
        print('Running on python interpreter version ' +
              str(sys.version_info.major) + '.' + str(sys.version_info.minor))
        print('\tPlatform: ' + platform.platform() + os.linesep +
              '\tMachine: ' + platform.machine())

    parser = argparse.ArgumentParser(prog=appname,
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     epilog=get_command_info_as_string())
    parser.add_argument('--version',
                        action='version',
                        version=__version__)
    parser.add_argument('--test',
                        action='store',
                        help='Just an arg')

    args = parser.parse_args(None if argv[0:] else ['--help'])

    logger = EmeraldLogger(logging_module_name='launcher')

    startup_time_utc = datetime.datetime.now(tz=timezone('UTC'))

    print('Args = ' + str(args.__dict__))
    logger.logger.warn('Initializing ' + appname + ' Version ' + __version__ + ', startup at ' +
                       datetime.datetime.strftime(startup_time_utc, '%Y%m%dT%H:%M:%S%z'))

VERSION = (0, 9, 1)
__version__ = '.'.join([str(x) for x in VERSION])


def print_version():
    print('Version = ' + __version__)

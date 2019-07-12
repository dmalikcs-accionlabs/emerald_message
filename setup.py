import os
from setuptools import setup, find_packages

__author__ = 'dthompson@dynastyse.com'


def get_version(version_tuple):
    if not isinstance(version_tuple[-1], int):
        return ','.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))


# we don't want to import the version file because this could cause problems in setup - read it explicitly instead
#  This will ensure we have access to __version__ variable (.e. ignore the unresolved reference error that may be thrown
#   in code inspection) - see line 17 for that fake error
#
init = os.path.join(os.path.dirname(__file__), 'emerald_message', 'version.py')
version_line = list(
    filter(lambda l: l.startswith('VERSION'), open(init)))[0]
VERSION = get_version(eval(version_line.split('=')[-1]))
print('Version = ' + VERSION)

#
# DET NOTE - remember that all SUB packages must be declared if they are to work when code is run from an egg
#  installed via easy_install.  Just use the "find_package()" routine to accomplish this
#
#  For installing bjoern on Mac OS X, you need to use a pip command with different path:
# pip install --global-option=build_ext --global-option="-I/usr/local/include/"
#       --global-option="-L/usr/local/lib"  bjoern
setup(
    name='emerald_message',
    packages=find_packages(),
    version=VERSION,
    description='Handler library and delivery tool for Emerald messages captured from email, SMS and other sources',
    author='Dave Thompson',
    author_email='dthompson@dynastyse.com',
    install_requires=[
        'avro-python3>=1.9.0',
        'azure-mgmt-storage>=4.0.0',
        'netaddr>=0.7.19',
        'pytz>=2019.1',
        'six>=1.12.0',
        'spooky>=2.0.0',
        'tzlocal>=1.5.1',
        'twine>=1.13.0',
        'Twisted>=19.2.1',
        'werkzeug>=0.15.4'
    ],
    include_package_data=True,
    zip_safe=True,
    url='http://www.dynastyse.com',
    license="LICENSE.txt",
    classifiers=[
        # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        # How mature is this project? Common values are
        # Development Status :: 1 - Planning
        # Development Status :: 2 - Pre-Alpha
        # Development Status :: 3 - Alpha
        # Development Status :: 4 - Beta
        # Development Status :: 5 - Production/Stable
        # Development Status :: 6 - Mature
        # Development Status :: 7 - Inactive
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Dynasty AutoProcessor Devops',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: System :: Systems Administration :: Devops'
    ]
)

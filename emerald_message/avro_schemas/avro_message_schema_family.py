import os
from enum import Enum, unique
from typing import FrozenSet
# noinspection PyCompatibility
from importlib.resources import path

from emerald_message import avro_schemas


@unique
class AvroMessageSchemaFamily(Enum):
    EMAIL = 'schema_email'

    @property
    def family_name(self) -> str:
        return self.value

    @classmethod
    def enumerate_schema_family_names(cls) -> FrozenSet[str]:
        schema_subfolders = []
        with path('emerald_message.avro_schemas', '') as schema_avro_path:
            with os.scandir(schema_avro_path) as subfolder_iterator:
                for entry in subfolder_iterator:
                    if not entry.name.startswith('.') and not entry.name.startswith('__') and entry.is_dir():
                        schema_subfolders.append(entry.name)
        return frozenset(schema_subfolders)

    #
    # use a check in the constructor to be sure that the values we set here actually match the folder names
    # underneath the avro_schemas package.  This will blow up at runtime if the valuees in enum above do not
    # match.  Doing this gives us a way to provide a well understood name like "EMAIL" that users of the package
    # can use to figure out what schemas are available and how to get the namespace identifiers
    #
    def __new__(cls, value):
        print('Initializing schema family value = ' + str(value))
        path_names = cls.enumerate_schema_family_names()
        # halt if we have a mismatch in the names
        if value not in path_names:
            raise RuntimeError('Code structure / naming error in ' + cls.__name__ + os.linesep + '"' +
                               str(value) + '" is not in the list of schema folders defined in the package' +
                               os.linesep + 'Valid paths (which will contain AVRO schemas) under code package "' +
                               avro_schemas.__name__ + '": ' + os.linesep +
                               ','.join(sorted([x for x in path_names])) + os.linesep +
                               'Check initialization of value in this class ' + cls.__name__ +
                               ' and in the folder names of included sub-package "' +
                               avro_schemas.__name__ + '", which should match' +
                               os.linesep + 'This provides a bridge for users of this python package to understand ' +
                               'how to reference the available schemas' + os.linesep +
                               'There is likely a typo or spelling error in one of the two')

        print('Will be initializing ' + cls.__name__ + ' with value ' + str(value))
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

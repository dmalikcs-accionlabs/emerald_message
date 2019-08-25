import os
import avro.schema
import json
from dataclasses import dataclass
# noinspection PyCompatibility
from importlib.resources import path
from typing import FrozenSet, List, Set, Union, Optional
from emerald_message import avro_schemas
from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.logging.logger import EmeraldLogger, EmeraldLoggerLevel
from emerald_message.error import EmeraldSchemaParsingException


#
#  DET TODO - eliminate this as we appear to be able  to store everything in the names
#
@dataclass(frozen=True)
class AvroMessageSchemaRecord:
    # even though the name is embedded in the schema, we need as a key when searching / using from other classes
    avro_schema_family_name: AvroMessageSchemaFamily
    avro_schema_name: str
    avro_schema: avro.schema.Schema


@dataclass(frozen=True)
class AvroMessageSchemaConfigurationRecord:
    debug: bool
    schema_subfolders: Optional[Union[List[str], Set[str]]] = None
    schema_extension: str = 'avsc'


#
#  DESIGN NOTE - we need the actual schema to be immutable as an object. We really don't need to instantiate it
#  over and over again, and in fact want to operate with this as class level abstractions only
#  Make sure none of the underlying elements are mutable
#
class AvroMessageSchemas:
    @property
    def debug(self) -> bool:
        return self._debug

    @property
    def logger(self) -> EmeraldLogger:
        return self._logger

    @property
    def schema_extension(self) -> str:
        return self._schema_extension

    '''
    This property is continuously updated as we initialize. It is a dictionary keyed by namespace with schema as value
    '''

    @property
    def known_avro_schema_dot_names(self) -> avro.schema.Names:
        return self._known_avro_schema_dot_names

    @property
    def avro_schema_list(self) -> List[avro.schema.Schema]:
        # schemas are not hashable so can't return a set
        return [v for k, v in self._known_avro_schema_dot_names.names.items()]

    @property
    def avro_schema_base_namespaces(self) -> FrozenSet[str]:
        # rememeber - the namespace property will not vary for elements within a given place - may only be one
        return frozenset([x.avro_schema.namespace for x in self._avro_schema_list])

    @property
    def avro_schema_namespaces(self) -> FrozenSet[str]:
        return frozenset([k for k, v in self.known_avro_schema_dot_names.names.items()])

    def get_schema_by_full_namespace(self,
                                     namespace: str) -> avro.schema.Schema:
        for this_namespace, this_schema in self.known_avro_schema_dot_names.names.items():
            if this_namespace == namespace:
                return this_schema

        raise KeyError('Unable to locate schema with namespace "' + str(namespace))

    def get_matching_schema_record_by_family_and_name(self,
                                                      schema_family: AvroMessageSchemaFamily,
                                                      schema_name: str) -> AvroMessageSchemaRecord:
        # iterate through and find our record - it is a kind of compound key (we won't have multiple names
        # in the same family
        for this_schema_record in self._avro_schema_list:
            if this_schema_record.avro_schema_family_name == schema_family and \
                    this_schema_record.avro_schema_name == schema_name:
                return this_schema_record

        raise KeyError('Unable to match schema entry against the matching criteria' +
                       os.linesep + '\tFamily to match: ' + schema_family.name +
                       os.linesep + '\tName to match: ' + schema_name +
                       os.linesep + 'Values known: ' + os.linesep + '\t' +
                       (os.linesep + '\t').join(
                           [x.avro_schema_family_name.name + ': ' + x.avro_schema_name
                            for x in self._avro_schema_list]))

    def get_schema_by_family_and_name(self,
                                      schema_family: AvroMessageSchemaFamily,
                                      schema_name: str) -> avro.schema.Schema:
        # propagate up the key error if we don't get a match
        return self.get_matching_schema_record_by_family_and_name(schema_family=schema_family,
                                                                  schema_name=schema_name).avro_schema

    def __init__(self,
                 schema_configuration_record: AvroMessageSchemaConfigurationRecord
                 ):
        if not isinstance(schema_configuration_record, AvroMessageSchemaConfigurationRecord):
            raise RuntimeError('Caller cannot initialize ' + AvroMessageSchemas.__name__ + ' - must provide ' +
                               'valid ' + AvroMessageSchemaConfigurationRecord.__name__ + ' in parameter ' +
                               'schema_configuration_record' + os.linesep +
                               'Type provided = ' + type(schema_configuration_record).__name__)
        # DET FIXME this is transitional logic as we migrate schemas to a class based abstraction instead of instance
        debug = schema_configuration_record.debug
        schema_extension = schema_configuration_record.schema_extension
        schema_subfolders = schema_configuration_record.schema_subfolders

        self._debug = debug

        if type(schema_extension) is not str:
            raise TypeError('Caller must provide a string specifying extension to be matched for avro schema files' +
                            os.linesep + 'Specify empty string if no extenesion' +
                            os.linesep + 'Type of data provided: ' + type(schema_extension).__name__)
        # allow a completely empty string to be used signifying no extension
        self._schema_extension = (os.extsep + schema_extension) \
            if (not schema_extension.startswith(os.extsep) and len(schema_extension) > 0) else schema_extension

        if not isinstance(schema_subfolders, list) and \
                not isinstance(schema_subfolders, set) and \
                schema_subfolders is not None:
            raise TypeError(
                'Caller must provide schema_subfolders parameter as None or initialize with a list or set of strings'
            )

        # if caller specifies None as the schema_subfolders parameter, we will search through every directory
        if schema_subfolders is None or len(schema_subfolders) == 0:
            schema_subfolders = sorted(AvroMessageSchemaFamily.enumerate_schema_family_names())
            print('Scanning all subfolders: ' + os.linesep + ','.join([x for x in schema_subfolders]))

        self._logger = EmeraldLogger(logging_module_name=type(self).__name__,
                                     global_logging_level=EmeraldLoggerLevel.DEBUG
                                     if self._debug
                                     else EmeraldLoggerLevel.INFO,
                                     console_logging_level=EmeraldLoggerLevel.DEBUG
                                     if self._debug
                                     else EmeraldLoggerLevel.INFO
                                     )

        # initialize the names registry - it will be updated
        self._known_avro_schema_dot_names: avro.schema.Names = avro.schema.Names()

        # create the dictionary we will use to key schemas by their name - it is two level dictionary keyed
        #  first by the schema folder as text and then by the schema Name property from in the properly validated file
        avro_schema_list: List[AvroMessageSchemaRecord] = []

        for schema_type_counter, this_schema_subfolder in enumerate(schema_subfolders, start=1):
            self._logger.logger.info('Starting scan of AVRO schema subfolder "' + this_schema_subfolder + '"')
            schema_base_import_path = avro_schemas
            try:
                # pep erroneously complains  about this syntax if you use avro_schemas without being in string
                with path('emerald_message.avro_schemas', this_schema_subfolder) as schema_subfolder:
                    self._logger.logger.debug('The AVRO schema folder path = ' + str(schema_subfolder) + os.linesep +
                                              'Type = ' + type(schema_subfolder).__name__)

                    # our pattern is we first pull files from any subfolders (no nesting)
                    # where those folders are sorting
                    # in alphabetic descending order.  That allows us to use multiple files
                    # and ensure we parse in correct order

                    # enumerate through and get all the schema files - the schema_subfolder is a PosixPath
                    #  PASS 1- just directories
                    subdirectory_list = []
                    self.logger.logger.debug('Will scan ' + str(schema_subfolder))
                    for entry in schema_subfolder.iterdir():
                        if not entry.name.startswith('.') and entry.is_dir():
                            subdirectory_list.append(entry.name)

                    # sort the list alphabetically
                    subdirectory_list = sorted(subdirectory_list, reverse=False)
                    self._logger.logger.info('For schema subfolder "' + schema_subfolder.name +
                                             '", the list of subfolders: ' + str(subdirectory_list))

                    # now build the list of paths to search starting with all subfolders
                    # (sorted alphabetically) and then
                    #  main path - that way people can implement a simple child-parent scheme where dependent schemas
                    #  can live in main path
                    schema_search_path_list = [os.path.join(schema_subfolder, x) for x in subdirectory_list]
                    schema_search_path_list.append(schema_subfolder)

                    self._logger.logger.debug('The schema paths to search: ' + str(schema_search_path_list))

                    for this_search_path_counter, this_search_path in enumerate(schema_search_path_list, start=1):
                        self.logger.logger.debug('Scanning search path #' + str(this_search_path_counter) + ': ' +
                                                 str(this_search_path))

                        #  ok to use glob and do without iterator since this is not large
                        with os.scandir(this_search_path) as avro_search_path_iterator:
                            for schema_file_counter, schema_file in enumerate(avro_search_path_iterator, start=1):
                                if schema_file.is_dir():
                                    self.logger.logger.debug('Skipping entry "' + os.path.basename(schema_file.name) +
                                                             '" for search path ' +
                                                             os.path.basename(this_search_path) +
                                                             ': is a directory and nesting not supported')
                                    continue
                                if not os.path.splitext(schema_file)[1] == self.schema_extension:
                                    self.logger.logger.debug('Skipping entry "' + os.path.basename(schema_file.name) +
                                                             '" for search path ' +
                                                             os.path.basename(this_search_path) + ': extension "' +
                                                             os.path.splitext(schema_file.path)[1] +
                                                             '" does not match schema extension  "' +
                                                             self.schema_extension + '"')
                                    continue

                                self.logger.logger.info(
                                    'Scanning file ' + schema_file.name + ' in path ' + this_search_path)

                                # now try to parse the avro schema.  Read json first and then
                                # call method that lets us pass
                                #  known names
                                try:
                                    schema_file_json = json.load(
                                        open(schema_file, encoding='utf-8', mode='r')
                                    )
                                    avro_schema = \
                                        avro.schema.SchemaFromJSONData(json_data=schema_file_json,
                                                                       names=self._known_avro_schema_dot_names)
                                except json.JSONDecodeError as jdex:
                                    raise EmeraldSchemaParsingException(
                                        'Schema file "' + os.path.basename(
                                            schema_file) + '" cannot be read as valid JSON' +
                                        os.linesep + 'Exception  info: ' + str(jdex.args))
                                except avro.schema.SchemaParseException as spex:
                                    raise EmeraldSchemaParsingException(
                                        'Schema file "' + os.path.basename(
                                            schema_file) + '" cannot be parsed as a valid AVRO schema' +
                                        os.linesep + 'Exception info: ' + str(spex.args))

                                self.logger.logger.debug('Schema file #' + str(schema_file_counter) + ' contents: ' +
                                                         os.linesep + str(avro_schema) + os.linesep +
                                                         'Type = ' + type(avro_schema).__name__)

                                #
                                # now load up the data in our collection - we will keep a key of these
                                #  by the "family name" which is our term for the folder designating groups
                                #  of these.  That will make it easier to get back data for users of this package
                                # Because we have built a special enum to "key" the folder names and hide the
                                #  abstraction, set up that value - this is what people will use to search fo
                                #  what they want as a tag - effective schema XYZ belongs to family ABC
                                #  Ex: AvroMessageSchemaFamily.name of EMAIL would be key for all in schema_email
                                #
                                avro_schema_list.append(
                                    AvroMessageSchemaRecord(avro_schema_family_name=
                                                            AvroMessageSchemaFamily(this_schema_subfolder),
                                                            avro_schema_name=avro_schema.name,
                                                            avro_schema=avro_schema)
                                )

            except (ModuleNotFoundError, AttributeError) as mfex:
                raise EmeraldSchemaParsingException('Unable to locate module / attribute "' +
                                                    str(schema_base_import_path) +
                                                    '" of type "' + type(schema_base_import_path).__name__ + '"' +
                                                    os.linesep + 'Exception type: ' + type(mfex).__name__)

            self.logger.logger.info('Total schema count deserialized: ' + str(len(avro_schema_list)))
            self._avro_schema_list = avro_schema_list
            self.logger.logger.info('Namespaces (based on our collection instance): ' + os.linesep +
                                    os.linesep.join([x for x in self.avro_schema_base_namespaces]))
            self.logger.logger.info('Known schema names (based on avro.schema.Names registry: ' + os.linesep +
                                    str(self.known_avro_schema_dot_names.names))

        return


#
#  Design note - by wrapping the schema in this dataclass, we are allowing users to create their
#  own (still essentially immutable in concept) avro schemas that vary the parameters of the config
#  record.  IN this case, though we are giving ourselves the set that we will use for real work
#  in any container
#
@dataclass(frozen=True)
class AvroMessageSchemaFrozen:
    avro_schema_collection: AvroMessageSchemas = \
        AvroMessageSchemas(AvroMessageSchemaConfigurationRecord(
            schema_subfolders=None,
            debug=False,
            schema_extension='.avsc')
        )

    def __init__(self):
        print('Initializing frozen')
        raise AssertionError('debug')

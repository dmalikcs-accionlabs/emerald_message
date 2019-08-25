import os
import base64
from dataclasses import dataclass
from abc import ABCMeta, abstractmethod
from typing import Dict, Any
from avro.datafile import DataFileWriter, DataFileException, DataFileReader
from avro.io import DatumReader, DatumWriter, AvroTypeException

from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.avro_schemas.avro_message_schemas import AvroMessageSchemas, \
    AvroMessageSchemaFrozen, AvroMessageSchemaRecord
from emerald_message.error import EmeraldMessageContainerInitializationError, \
    EmeraldMessageSerializationError, EmeraldMessageDeserializationError

'''
Use this matching identifiere as a way of providing configuration parameters to the classes implementing
AbstractContainer - this ties to the underlying AVRO abstractions
'''


@dataclass(frozen=True)
class ContainerSchemaMatchingIdentifier:
    container_avro_schema_family_name: AvroMessageSchemaFamily
    container_avro_schema_name: str


'''
Use this as a base class to hold immutable parameters for each class implementing AbstractContainer
Remember the MRO scheme in dataclasses - ok to have parameters with and without defaults in the child
classes so long as you do not have default values here
'''


@dataclass(frozen=True)
class ContainerParameters:
    pass


class AbstractContainer(metaclass=ABCMeta):
    @property
    def debug(self) -> bool:
        return self._debug

    #  define utility function that do base 64 encoding -  if string will encode.  if bytes, assumes it is already done
    @classmethod
    def transform_base_64_encode(cls,
                                 element: str,
                                 encoding: str = 'utf-8'):
        if type(element) is not str:
            raise TypeError('Caller must provide str element to use base64 encode routine in ' +
                            cls.__name__ + os.linesep +
                            'Type provided = ' + type(element).__name__)

        return base64.b64encode(element.encode(encoding))

    @classmethod
    def transform_base_64_decode(cls,
                                 element: bytes,
                                 encoding: str = 'utf-8'):
        if not type(element) is bytes:
            raise TypeError('Caller must provide element as type bytes to use base64 encoding for ' +
                            'base 64 decoder in ' + cls.__name__ +
                            os.linesep + 'Type provided = ' + type(element).__name__)
        return base64.b64decode(bytes).decode(encoding)

    # we define in one place the mechanism for getting the AvroMessageSchemaRecord but note
    #  how it depends on the abstract methods implemented by the implementing classes
    #  Because the classes implementing this abstract class are dataclasses, you can't set properties in init
    @classmethod
    def get_avro_schema_record(cls) -> AvroMessageSchemaRecord:
        return AvroMessageSchemaFrozen.avro_schema_collection.get_matching_schema_record_by_family_and_name(
            schema_family=
            cls.get_container_schema_matching_identifier().container_avro_schema_family_name,
            schema_name=cls.get_container_schema_matching_identifier().container_avro_schema_name)

    def _get_container_parameters(self):
        return self._container_parameters

    # The implementing classes will use this write_avro and set their data_dictionary based on parameters
    #  Then they will call _write_avro_data to keep one implementation
    @abstractmethod
    def write_avro(self,
                   avro_container_uri: str):
        pass

    def _write_avro_data(self,
                         avro_container_uri: str,
                         data_as_dictionary: Dict[str, Any]):
        if type(avro_container_uri) is not str or len(avro_container_uri) == 0:
            raise EmeraldMessageSerializationError(
                'Unable to write avro - avro_container_uri parameter' +
                ' must be a string specifying container location in writable form')
        if type(data_as_dictionary) is not dict:
            raise EmeraldMessageSerializationError(
                'Unable to write avro - data_dictionary parameter is incorrect type ("' +
                type(data_as_dictionary).__name__ + os.linesep + 'Should be a dictionary of k,v data pairs'
            )
        if self.debug:
            print('Avro schema type = ' + str(type(type(self).get_avro_schema_record().avro_schema)))
            print('Avro schema = ' + str(type(self).get_avro_schema_record().avro_schema))

        with open(avro_container_uri, "wb") as writer_fp:
            with DataFileWriter(writer_fp,
                                DatumWriter(),
                                type(self).get_avro_schema_record().avro_schema) as writer:
                if self.debug:
                    print('Opened data file write')
                try:
                    writer.append(data_as_dictionary)
                except AvroTypeException as iex:
                    raise EmeraldMessageSerializationError(
                        'Unable to serialize object of type ' +
                        type(self).__name__ + ' due to data mismatch in Avro schema' +
                        os.linesep + 'Error info: ' + str(iex.args[0]))
        return

    #
    # DET NOTE - implementing classes will get back the datum by calling _from_avro_generic and populating
    #  we could implement a more abstract version that initializes using getattr and setattr but
    #  that can come at a later time if needed
    #
    @staticmethod
    @abstractmethod
    def from_avro(avro_container_uri: str,
                  debug: bool = False):
        pass

    @staticmethod
    def _from_avro_generic(
            avro_container_uri: str,
    ):
        datum_counter = 0
        datum_to_return = None
        # DET TODO add other exception handling around the double with clause
        with open(avro_container_uri, "rb") as avro_fp:
            with DataFileReader(avro_fp, DatumReader()) as reader:
                #
                #  This static meethod can only initialize one datum in the file - scan through and raise
                #  error if more than one found
                #  Not sure if there is lazy access to the datum - if so returning the datum to caller
                #  for subsequent loading would be problematic
                #
                for datum_counter, datum in enumerate(reader, start=1):
                    print('Reading datum #' + str(datum_counter))
                    print('The message datum = ' + str(datum))
                    if datum_counter == 1:
                        datum_to_return = datum

        if datum_counter > 1:
            raise EmeraldMessageDeserializationError(
                'Unable to deserialize from AVRO container "' +
                avro_container_uri + '" - this deserializer can only have one datum per file' +
                os.linesep + 'Total element count in this file = ' + str(datum_counter))

        if datum_to_return is None:
            raise EmeraldMessageDeserializationError(
                'Data could not be loaded from AVRO file "' + str(avro_container_uri) +
                '" using schema ' + AbstractContainer.get_avro_schema_record().avro_schema_name)

        print('Length of datum to return = ' + str(datum_to_return))
        print('Type of data  to return = ' + str(type(datum_to_return)))
        return datum_to_return

    @classmethod
    @abstractmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        pass

    @classmethod
    @abstractmethod
    def _get_container_parameters_required_subclass_type(cls):
        pass

    def __init__(self,
                 container_parameters: ContainerParameters,
                 debug: bool = False):

        # The implementing class must determine the namespace for the schema corresponding to this
        #  This checking really happens after the fact but we provide just in case there are problems
        #  with a class not implementing one of the required methods
        #
        #  There is still limited use for it - if someone literally sets the wrong kind of object in the
        #  class contract such as get_container_schema_matching_identifier, these errors will trip and halt
        #  further initialization
        #
        #  For this container parameters, our implementation will be different in each class, but we at leeast
        #  know it is a named tuple
        #
        if not isinstance(container_parameters, ContainerParameters):
            raise EmeraldMessageContainerInitializationError(
                'Caller must provide value for container_parameters ' +
                'as an object extending the class "' +
                ContainerParameters.__name__ + os.linesep +
                '" Type provided was ' +
                type(container_parameters).__name__)
        # here we make sure that the container parameters passed in is the correct type to avoid odd
        #  initialization errors
        if type(container_parameters) != type(self)._get_container_parameters_required_subclass_type():
            raise EmeraldMessageContainerInitializationError(
                'Initialization parameter container_parameters is ' +
                'of wrong type for class ' + type(self).__name__ +
                os.linesep + 'Parameter type = ' +
                str(type(container_parameters)) +
                os.linesep + 'Required type = ' +
                str(type(self)._get_container_parameters_required_subclass_type()) +
                os.linesep + 'Check implementation of method ' +
                type(self)._get_container_parameters_required_subclass_type.__name__ +
                os.linesep + 'and verify initialization in class ' + type(self).__name__ + ' is correct')

        self._container_parameters = container_parameters

        if not isinstance(type(self).get_container_schema_matching_identifier(), ContainerSchemaMatchingIdentifier):
            raise EmeraldMessageContainerInitializationError(
                'Caller must provide the value for ' + ContainerSchemaMatchingIdentifier.__name__ +
                ' in order to identify the proper AVRO schema for the container')

        if type(type(self).get_container_schema_matching_identifier().container_avro_schema_name) is not str or \
                len(type(self).get_container_schema_matching_identifier().container_avro_schema_name) == 0:
            raise EmeraldMessageContainerInitializationError(
                'Caller must provide a valid name corresponding to a name property in a defined AVRO' +
                ' schema that corresponds to this container')

        print('Initialized the avro schema record to ' + str(type(self).get_avro_schema_record()))
        self._debug = debug

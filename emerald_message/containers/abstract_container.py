import os
import avro.schema
from dataclasses import dataclass
from abc import ABCMeta, abstractmethod
from typing import Optional

from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.avro_schemas.avro_message_schemas import AvroMessageSchemas, \
    AvroMessageSchemaFrozen, AvroMessageSchemaRecord
from emerald_message.error import EmeraldMessageContainerInitializationError


@dataclass(frozen=True)
class ContainerSchemaMatchingIdentifier:
    container_avro_schema_family_name: AvroMessageSchemaFamily
    container_avro_schema_name: str


class AbstractContainer(metaclass=ABCMeta):
    @property
    def debug(self) -> bool:
        return self._debug

    # we define in one place the mechanism for getting the AvroMessageSchemaRecord but note
    #  how it depends on the abstract methods implemented by the implementing classes
    #  Because the classes implementing this abstract class are dataclasses, you can't set properties in init
    @classmethod
    def get_avro_schema_record(cls) -> AvroMessageSchemaRecord:
        return AvroMessageSchemaFrozen.avro_schema_collection.get_matching_schema_record_by_family_and_name(
            schema_family=
            cls.get_container_schema_matching_identifier().container_avro_schema_family_name,
            schema_name=cls.get_container_schema_matching_identifier().container_avro_schema_name)

    @abstractmethod
    def write_avro(self,
                   avro_container_uri: str):
        pass

    @staticmethod
    @abstractmethod
    def from_avro(avro_container_uri: str):
        pass

    @classmethod
    @abstractmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        pass

    def __init__(self,
                 debug: bool = False):
        # The implementing class must determine the namespace for the schema corresponding to this
        #  This checking really happens after the fact but we provide just in case there are problems
        #  with a class not implementing one of the required methods
        #
        #  There is still limited use for it - if someone literally sets the wrong kind of object in the
        #  class contract such as get_container_schema_matching_identifier, these errors will trip and halt
        #  further initialization
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
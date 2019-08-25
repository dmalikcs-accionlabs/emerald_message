import os
from dataclasses import dataclass
from typing import FrozenSet, Dict

from emerald_message.containers.abstract_container import AbstractContainer, ContainerSchemaMatchingIdentifier, \
    ContainerParameters
from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.error import EmeraldMessageDeserializationError


@dataclass(frozen=True)
class EmailEnvelopeParameters(ContainerParameters):
    address_from: str
    address_to_collection: FrozenSet[str]
    message_subject: str
    message_rx_timestamp_iso8601: str


class EmailEnvelope(AbstractContainer):
    @property
    def address_from(self) -> str:
        return self._get_container_parameters().address_from

    @property
    def address_to_collection(self) -> FrozenSet[str]:
        return self._get_container_parameters().address_to_collection

    @property
    def message_subject(self) -> str:
        return self._get_container_parameters().message_subject

    @property
    def message_rx_timestamp_iso8601(self) -> str:
        return self._get_container_parameters().message_rx_timestamp_iso8601

    # order doesn't actually matter in the "address_to_collection" but we need to make sure we sort
    #  alphabetically when converting to string so the sort will always work on same order and be idempotent
    #  straight comparisons of frozensets (x == y) do not require sorting, but string conversions do
    #  so we can account for 1,2,3 being equal hash-wise to 3,2,1
    def __str__(self):
        return 'FROM: "' + self.address_from + os.linesep + \
               'TO: "' + ','.join([str(x) for x in sorted(self.address_to_collection)]) + '"' + os.linesep + \
               'SUBJECT: "' + self.message_subject + '"' + os.linesep + \
               'RECEIVED: "' + self.message_rx_timestamp_iso8601 + '"'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if not isinstance(other, EmailEnvelope):
            return False

        if self.address_from != other.address_from:
            return False

        # as with the string conversion, we need to sort the frozensets when doing < or > tests
        #  it is ok to skip wnen doing equality tests
        sorted_self = sorted(self.address_to_collection)
        sorted_other = sorted(other.address_to_collection)
        if sorted_self != sorted_other:
            return False

        if self.message_rx_timestamp_iso8601 != other.message_rx_timestamp_iso8601:
            return False

        return True

    def __ne__(self, other):
        return not  self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, EmailEnvelope):
            raise TypeError('Cannot compare object of type "' + type(other).__name__ + '" to ' +
                            EmailEnvelope.__name__)

        if self.address_from < other.address_from:
            return True
        elif self.address_from > other.address_from:
            return False

        # as with the string conversion, we need to sort the frozensets when doing < or > tests
        #  it is ok to skip wnen doing equality tests
        sorted_self = sorted(self.address_to_collection)
        sorted_other = sorted(other.address_to_collection)
        if sorted_self < sorted_other:
            return True
        elif sorted_self > sorted_other:
            return False

        if self.message_subject < other.message_subject:
            return True
        elif self.message_subject > other.message_subject:
            return False

        if self.message_rx_timestamp_iso8601 < other.message_rx_timestamp_iso8601:
            return True
        elif self.message_rx_timestamp_iso8601 > other.message_rx_timestamp_iso8601:
            return False

        return False

    def __gt__(self, other):
        if not isinstance(other, EmailEnvelope):
            raise TypeError('Cannot compare object of type "' + type(other).__name__ + '" to ' +
                            EmailEnvelope.__name__)

        if self.address_from > other.address_from:
            return True
        elif self.address_from < other.address_from:
            return False

        # as with the string conversion, we need to sort the frozensets when doing < or > tests
        #  it is ok to skip wnen doing equality tests
        sorted_self = sorted(self.address_to_collection)
        sorted_other = sorted(other.address_to_collection)
        if sorted_self > sorted_other:
            return True
        elif sorted_self < sorted_other:
            return False

        if self.message_subject > other.message_subject:
            return True
        elif self.message_subject < other.message_subject:
            return False

        if self.message_rx_timestamp_iso8601 > other.message_rx_timestamp_iso8601:
            return True
        elif self.message_rx_timestamp_iso8601 < other.message_rx_timestamp_iso8601:
            return False

        return False

    def __ge__(self, other):
        return not self.__lt__(other)

    def __le__(self, other):
        return not self.__gt__(other)

    @classmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        return ContainerSchemaMatchingIdentifier(
            container_avro_schema_family_name=AvroMessageSchemaFamily.EMAIL,
            container_avro_schema_name='EmailEnvelope'
        )

    @classmethod
    def _get_container_parameters_required_subclass_type(cls):
        return EmailEnvelopeParameters

    def get_as_dict(self) -> Dict:
        return \
            {
                "address_from": self.address_from,
                "address_to_collection": sorted(self.address_to_collection),
                "message_subject": self.message_subject,
                "message_rx_timestamp_iso8601": self.message_rx_timestamp_iso8601
            }

    def write_avro(self,
                   avro_container_uri: str):
        # remember that the array of addreess_to_collection must go out as a list to be serialized by the python
        #  library for Avro.  In all our comparison code for this class we always convert from frozenset  to list
        #  and sort for purposes of comparison
        type(self)._write_avro_data(self,
                                    data_as_dictionary=self.get_as_dict(),
                                    avro_container_uri=avro_container_uri)

    @staticmethod
    def from_avro_as_dict(avro_parameter_dict: Dict):
        try:
            new_email_envelope = \
                EmailEnvelope(
                    container_parameters=
                    EmailEnvelopeParameters(
                        address_from=avro_parameter_dict['address_from'],
                        address_to_collection=frozenset(avro_parameter_dict['address_to_collection']),
                        message_subject=avro_parameter_dict['message_subject'],
                        message_rx_timestamp_iso8601=avro_parameter_dict['message_rx_timestamp_iso8601']
                    )
                )
        except KeyError as kex:
            raise EmeraldMessageDeserializationError(
                'Unable to load object from AVRO dictionary ' + os.linesep + str(avro_parameter_dict) +
                os.linesep + 'Unable to locate one or more keys in the data' +
                os.linesep + 'Returned data parameter count = ' + str(len(avro_parameter_dict)) +
                os.linesep + 'Cannot locate key "' + str(kex.args[0]) + '" in data' +
                os.linesep + 'Key(s) found: ' + ','.join([str(k) for k, v in avro_parameter_dict.items()])
            )

        return new_email_envelope

    @staticmethod
    def from_avro(avro_container_uri: str,
                  debug: bool = False):
        # pass up the exceptions
        datum_to_load = AbstractContainer._from_avro_generic(avro_container_uri=avro_container_uri)

        return EmailEnvelope.from_avro_as_dict(datum_to_load)

    #
    #  see design note inside the constructor - the parameters really could have been named tuples just
    #  as easily as dataclass with frozen in this case, because of how we used
    #  But dataclassses let us subclass for better type checking
    #
    def __init__(self,
                 container_parameters: ContainerParameters):
        # we want both immutable parameters and ability to extend this class from abstract
        #  as of 201908, only pattern DET sees is to store them in class as we would a named tuple
        #  otherwise we cannot set other instance parameters in here because without separate
        #  instance parameter "container_parameter" we'd end up setting dataclass to true on this actual class
        #
        print('initializing with ' + str(container_parameters))
        super(EmailEnvelope, self).__init__(container_parameters=container_parameters)

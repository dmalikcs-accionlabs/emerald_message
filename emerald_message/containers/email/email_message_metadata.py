import os
from dataclasses import dataclass
from typing import Optional, Dict
from emerald_message.containers.abstract_container import AbstractContainer, ContainerSchemaMatchingIdentifier, \
    ContainerParameters
from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.error import EmeraldMessageDeserializationError
from netaddr import IPAddress


@dataclass(frozen=True)
class EmailMessageMetadataParameters(ContainerParameters):
    router_source_tag: str
    routed_timestamp_iso8601: str
    email_sender_ip: IPAddress
    attachment_count: int
    email_headers: str
    email_spf_sender_passed: Optional[bool] = None
    email_dkim_sender_passed: Optional[bool] = None


class EmailMessageMetadata(AbstractContainer):
    @property
    def router_source_tag(self) -> str:
        return self._get_container_parameters().router_source_tag

    @property
    def routed_timestamp_iso8601(self) -> str:
        return self._get_container_parameters().routed_timestamp_iso8601

    @property
    def email_sender_ip(self) -> IPAddress:
        return self._get_container_parameters().email_sender_ip

    @property
    def attachment_count(self) -> int:
        return self._get_container_parameters().attachment_count

    @property
    def email_headers(self) -> str:
        return self._get_container_parameters().email_headers

    @property
    def email_spf_sender_passed(self) -> Optional[bool]:
        return self._get_container_parameters().email_spf_sender_passed

    @property
    def email_dkim_sender_passed(self) -> Optional[bool]:
        return self._get_container_parameters().email_dkim_sender_passed

    @property
    def authentication_filters_state(self) -> Optional[bool]:
        # this is a tristate, meaning we need actual booleans for both in order to and the answer
        # otherwise we return None as a "do not know:
        if self.email_dkim_sender_passed is None or self.email_spf_sender_passed is None:
            return None

        return self.email_spf_sender_passed and self.email_dkim_sender_passed

    def __str__(self):
        # this is not necessarily useful for logging, but is needed for hashing
        return \
            'Router Source Tag: ' + str(self.router_source_tag) + os.linesep + \
            'Routed Timestamp ISO8601: ' + str(self.routed_timestamp_iso8601) + os.linesep + \
            'Sender IP Address: ' + str(self.email_sender_ip) + os.linesep + \
            'Attachment Count: ' + str(self.attachment_count) + os.linesep + \
            'Authentication SPF check: ' + \
            ('Not Available' if self.email_spf_sender_passed is None else str(self.email_spf_sender_passed)) + \
            'Authentication DKIM check: ' + \
            ('Not Available' if self.email_dkim_sender_passed is None else str(self.email_dkim_sender_passed)) + \
            'Email Headers: ' + os.linesep + str(self.email_headers) + os.linesep

    def __hash__(self):
        # use a regular hash for this as it is not too long
        return hash(str(self))

    def __eq__(self, other):
        if not isinstance(other, EmailMessageMetadata):
            return False

        if self.router_source_tag != other.router_source_tag:
            return False

        if self.routed_timestamp_iso8601 != other.routed_timestamp_iso8601:
            return False

        if self.email_sender_ip != other.email_sender_ip:
            return False

        if self.attachment_count != other.attachment_count:
            return False

        if self.email_headers != other.email_headers:
            return False

        if self.email_spf_sender_passed != other.email_spf_sender_passed:
            return False

        if self.email_dkim_sender_passed != other.email_dkim_sender_passed:
            return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, EmailMessageMetadata):
            raise TypeError('Cannot compare object of type "' + type(other).__name__ + '" to ' +
                            EmailMessageMetadata.__name__)

        if self.router_source_tag < other.router_source_tag:
            return True
        elif self.router_source_tag > other.router_source_tag:
            return False

        if self.routed_timestamp_iso8601 < other.routed_timestamp_iso8601:
            return True
        elif self.routed_timestamp_iso8601 > other.routed_timestamp_iso8601:
            return False

        if self.email_sender_ip < other.email_sender_ip:
            return True
        elif self.email_sender_ip > other.email_sender_ip:
            return False

        if self.attachment_count < other.attachment_count:
            return True
        elif self.attachment_count > other.attachment_count:
            return False

        if self.email_headers < other.email_headers:
            return True
        elif self.email_headers > other.email_headers:
            return False

        try:
            if self.email_spf_sender_passed < other.email_spf_sender_passed:
                return True
            elif self.email_spf_sender_passed > other.email_spf_sender_passed:
                return False
        except TypeError:
            # for optional booleans we have to check for a type error - None cannot be compared
            #  False < True,  Skip if neither < or <
            #  We will designate True > False > None to make things idempotent
            if self.email_spf_sender_passed is None:
                if other.email_spf_sender_passed is not None:
                    return True
            elif other.email_spf_sender_passed is None:
                if self.email_spf_sender_passed is not None:
                    return False
            pass

        try:
            if self.email_dkim_sender_passed < other.email_dkim_sender_passed:
                return True
            elif self.email_dkim_sender_passed > other.email_dkim_sender_passed:
                return False
        except TypeError:
            # for optional booleans we have to check for a type error - None cannot be compared
            #  False < True,  Skip if neither < or <
            #  We will designate True > False > None to make things idempotent
            if self.email_dkim_sender_passed is None:
                if other.email_dkim_sender_passed is not None:
                    return True
            elif other.email_dkim_sender_passed is None:
                if self.email_dkim_sender_passed is not None:
                    return False
            pass

        # all are equal so false
        return False

    def __gt__(self, other):
        if not isinstance(other, EmailMessageMetadata):
            raise TypeError('Cannot compare object of type "' + type(other).__name__ + '" to ' +
                            EmailMessageMetadata.__name__)

        if self.router_source_tag > other.router_source_tag:
            return True
        elif self.router_source_tag < other.router_source_tag:
            return False

        if self.routed_timestamp_iso8601 > other.routed_timestamp_iso8601:
            return True
        elif self.routed_timestamp_iso8601 < other.routed_timestamp_iso8601:
            return False

        if self.email_sender_ip > other.email_sender_ip:
            return True
        elif self.email_sender_ip < other.email_sender_ip:
            return False

        if self.attachment_count > other.attachment_count:
            return True
        elif self.attachment_count < other.attachment_count:
            return False

        if self.email_headers > other.email_headers:
            return True
        elif self.email_headers < other.email_headers:
            return False

        try:
            if self.email_spf_sender_passed > other.email_spf_sender_passed:
                return True
            elif self.email_spf_sender_passed < other.email_spf_sender_passed:
                return False
        except TypeError:
            # for optional booleans we have to check for a type error - None cannot be compared
            #  False < True,  Skip if neither < or <
            #  We will designate True > False > None to make things idempotent
            if self.email_spf_sender_passed is None:
                if other.email_spf_sender_passed is not None:
                    return False
            elif other.email_spf_sender_passed is None:
                if self.email_spf_sender_passed is not None:
                    return True
            pass

        try:
            if self.email_dkim_sender_passed > other.email_dkim_sender_passed:
                return True
            elif self.email_dkim_sender_passed < other.email_dkim_sender_passed:
                return False
        except TypeError:
            # for optional booleans we have to check for a type error - None cannot be compared
            #  False < True,  Skip if neither < or <
            #  We will designate True > False > None to make things idempotent
            if self.email_dkim_sender_passed is None:
                if other.email_dkim_sender_passed is not None:
                    return False
            elif other.email_dkim_sender_passed is None:
                if self.email_dkim_sender_passed is not None:
                    return True
            pass

        # all are equal so false
        return False

    def __ge__(self, other):
        return not self.__lt__(other)

    def __le__(self, other):
        return not self.__gt__(other)

    @classmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        return ContainerSchemaMatchingIdentifier(
            container_avro_schema_family_name=AvroMessageSchemaFamily.EMAIL,
            container_avro_schema_name='EmailMessageMetadata'
        )

    @classmethod
    def _get_container_parameters_required_subclass_type(cls):
        return EmailMessageMetadataParameters

    def get_as_dict(self) -> Dict:
        return \
            {
                "router_source_tag": self.router_source_tag,
                "routed_timestamp_iso8601": self.routed_timestamp_iso8601,
                "email_sender_ip": str(self.email_sender_ip),
                "attachment_count": self.attachment_count,
                "email_headers": self.email_headers,
                "email_spf_sender_passed": self.email_spf_sender_passed,
                "email_dkim_sender_passed": self.email_dkim_sender_passed
            }

    def write_avro(self,
                   avro_container_uri: str):
        # remember that the array of addreess_to_collection must go out as a list to be serialized by the python
        #  library for Avro.  In all our comparison code for this class we always convert from frozenset  to list
        #  and sort for purposes of comparison
        #  We serialize IP addreess into a string
        type(self)._write_avro_data(self,
                                    data_as_dictionary=self.get_as_dict(),
                                    avro_container_uri=avro_container_uri)

    @staticmethod
    def from_avro_as_dict(avro_parameter_dict: Dict):
        # we deserialize the IP address from a string
        try:
            new_email_message_metadata = \
                EmailMessageMetadata(
                    container_parameters=
                    EmailMessageMetadataParameters(
                        router_source_tag=avro_parameter_dict['router_source_tag'],
                        routed_timestamp_iso8601=avro_parameter_dict['routed_timestamp_iso8601'],
                        email_sender_ip=IPAddress(avro_parameter_dict['email_sender_ip']),
                        attachment_count=avro_parameter_dict['attachment_count'],
                        email_headers=avro_parameter_dict['email_headers'],
                        email_spf_sender_passed=avro_parameter_dict['email_spf_sender_passed'],
                        email_dkim_sender_passed=avro_parameter_dict['email_dkim_sender_passed']
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
        return new_email_message_metadata


    @staticmethod
    def from_avro(avro_container_uri: str):
        # pass up the exceptions
        datum_to_load = AbstractContainer._from_avro_generic(avro_container_uri=avro_container_uri)

        return EmailMessageMetadata.from_avro_as_dict(datum_to_load)

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
        super(EmailMessageMetadata, self).__init__(container_parameters=container_parameters)

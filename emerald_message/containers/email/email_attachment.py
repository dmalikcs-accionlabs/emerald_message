import os
from spooky import hash128
from dataclasses import dataclass
from typing import Dict

from emerald_message.containers.abstract_container import AbstractContainer, ContainerSchemaMatchingIdentifier, \
    ContainerParameters
from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.error import EmeraldMessageDeserializationError


@dataclass(frozen=True)
class EmailAttachmentParameters(ContainerParameters):
    filename: str
    mimetype: str
    contents_base64: str


class EmailAttachment(AbstractContainer):
    @property
    def filename(self) -> str:
        return self._get_container_parameters().filename

    @property
    def mimetype(self) -> str:
        return self._get_container_parameters().mimetype

    @property
    def contents_base64(self) -> str:
        return self._get_container_parameters().contents_base64

    @classmethod
    def _get_container_parameters_required_subclass_type(cls):
        return EmailAttachmentParameters

    def __str__(self):
        print('The type of content = ' + type(self.contents_base64).__name__ +
              os.linesep + 'The value = ' + str(self.contents_base64))
        return 'Filename: "' + str(self.filename) + '"' + os.linesep + \
               'Mimetype: "' + str(self.mimetype) + '"' + os.linesep + \
               'Content length: "' + str(len(self.contents_base64)) + '"' + os.linesep + \
               'Content hash128"' + str(hash128(self.contents_base64)).encode('utf-8').hex() + '"'

    # use spooky hash in 128 bit length as the attachments could be quite large - no collisions!
    #  then we can hash the string rendering normally
    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if not isinstance(other, EmailAttachment):
            return False

        if self.filename != other.filename:
            return False

        if self.mimetype != other.mimetype:
            return False

        # use the hash comparison so we have consistent use of spooky hash
        if hash128(self.contents_base64) != hash128(other.contents_base64):
            return False

        return True

    def __ne__(self, other):
        return not  self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, EmailAttachment):
            raise TypeError('Cannot compare object of type "' + type(other).__name__ + '" to ' +
                            EmailAttachment.__name__)

        if self.filename < other.filename:
            return True
        elif self.filename > other.filename:
            return False

        if self.mimetype < other.mimetype:
            return True
        elif self.mimetype > other.mimetype:
            return False

        # first check length of body base64
        if len(self.contents_base64) < len(other.contents_base64):
            return True
        elif len(self.contents_base64) > len(other.contents_base64):
            return False

        # now if we compare a gigantic attachment literally it will take forever so compare hashes instead
        hash_self = hash128(self.contents_base64)
        hash_other = hash128(other.contents_base64)
        if hash_self < hash_other:
            return True
        elif hash_self > hash_other:
            return False

        # getting here means they are equal
        return False

    def __gt__(self, other):
        if not isinstance(other, EmailAttachment):
            raise TypeError('Cannot compare object of type "' + type(other).__name__ + '" to ' +
                            EmailAttachment.__name__)

        if self.filename > other.filename:
            return True
        elif self.filename < other.filename:
            return False

        if self.mimetype > other.mimetype:
            return True
        elif self.mimetype < other.mimetype:
            return False

        # first check length of body base64
        if len(self.contents_base64) > len(other.contents_base64):
            return True
        elif len(self.contents_base64) < len(other.contents_base64):
            return False

        # now if we compare a gigantic attachment literally it will take forever so compare hashes instead
        hash_self = hash128(self.contents_base64)
        hash_other = hash128(other.contents_base64)
        if hash_self > hash_other:
            return True
        elif hash_self < hash_other:
            return False

        # getting here means they are equal
        return False

    def __ge__(self, other):
        return not self.__lt__(other)

    def __le__(self, other):
        return not self.__gt__(other)

    @classmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        return ContainerSchemaMatchingIdentifier(
            container_avro_schema_family_name=AvroMessageSchemaFamily.EMAIL,
            container_avro_schema_name='EmailAttachment'
        )

    def get_as_dict(self) -> Dict:
        return \
            {
                "filename": self.filename,
                "mimetype": self.mimetype,
                "contents_base64": self.contents_base64
            }

    def write_avro(self,
                   avro_container_uri: str):
        type(self)._write_avro_data(self,
                                    avro_container_uri=avro_container_uri,
                                    data_as_dictionary=self.get_as_dict()
                                    )

    @staticmethod
    def from_avro_as_dict(avro_parameter_dict: Dict):
        try:
            new_email_attachment = \
                EmailAttachment(container_parameters=
                                EmailAttachmentParameters(filename=avro_parameter_dict['filename'],
                                                          mimetype=avro_parameter_dict['mimetype'],
                                                          contents_base64=avro_parameter_dict['contents_base64']))
        except KeyError as kex:
            raise EmeraldMessageDeserializationError(
                'Unable to load object from AVRO dictionary ' + os.linesep + str(avro_parameter_dict) +
                os.linesep + 'Unable to locate one or more keys in the data' +
                os.linesep + 'Returned data parameter count = ' + str(len(avro_parameter_dict)) +
                os.linesep + 'Cannot locate key "' + str(kex.args[0]) + '" in data' +
                os.linesep + 'Key(s) found: ' + ','.join([str(k) for k, v in avro_parameter_dict.items()])
            )
        return new_email_attachment

    @staticmethod
    def from_avro(avro_container_uri: str):
        # pass up the exceptions
        datum_to_load = AbstractContainer._from_avro_generic(avro_container_uri=avro_container_uri)

        return EmailAttachment.from_avro_as_dict(datum_to_load)

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
        super(EmailAttachment, self).__init__(container_parameters=container_parameters)

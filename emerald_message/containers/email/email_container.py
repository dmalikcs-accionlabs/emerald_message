import os
from dataclasses import dataclass
from typing import FrozenSet, Dict

from emerald_message.containers.abstract_container import AbstractContainer, ContainerSchemaMatchingIdentifier, \
    ContainerParameters
from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily
from emerald_message.error import EmeraldMessageDeserializationError
from emerald_message.containers.email.email_attachment import EmailAttachment
from emerald_message.containers.email.email_body import EmailBody
from emerald_message.containers.email.email_envelope import EmailEnvelope
from emerald_message.containers.email.email_message_metadata import EmailMessageMetadata


@dataclass(frozen=True)
class EmailContainerParameters(ContainerParameters):
    email_message_metadata: EmailMessageMetadata
    email_envelope: EmailEnvelope
    email_body: EmailBody
    email_attachment_collection: FrozenSet[EmailAttachment]


class EmailContainer(AbstractContainer):
    @property
    def email_message_metadata(self) -> EmailMessageMetadata:
        return self._get_container_parameters().email_message_metadata

    @property
    def email_envelope(self) -> EmailEnvelope:
        return self._get_container_parameters().email_envelope

    @property
    def email_body(self) -> EmailBody:
        return self._get_container_parameters().email_body

    @property
    def email_attachment_collection(self) -> FrozenSet[EmailAttachment]:
        return self._get_container_parameters().email_attachment_collection

    def __str__(self):
        return \
            'Email Container' + os.linesep + \
            'Email Envelope: ' + os.linesep + str(self.email_envelope) + os.linesep + \
            'Email Body' + os.linesep + str(self.email_body) + os.linesep + \
            'Email Message Metadata' + os.linesep + str(self.email_message_metadata) + os.linesep + \
            'Email Attachment Collection' + os.linesep + str(sorted(self.email_attachment_collection))

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if not isinstance(other, EmailContainer):
            return False

        if self.email_body != other.email_body:
            return False

        if self.email_envelope != other.email_envelope:
            return False

        if self.email_message_metadata != other.email_message_metadata:
            return False

        if sorted(self.email_attachment_collection) != sorted(other.email_attachment_collection):
            return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        return ContainerSchemaMatchingIdentifier(
            container_avro_schema_family_name=AvroMessageSchemaFamily.EMAIL,
            container_avro_schema_name='EmailContainer'
        )

    @classmethod
    def _get_container_parameters_required_subclass_type(cls):
        return EmailContainerParameters

    def get_as_dict(self) -> Dict:
        # notice that since we have an array of attachments, we need to iterate through each,
        #  turning them into dictionaries and building into a list for serialization
        return \
            {
                "email_message_metadata": self.email_message_metadata.get_as_dict(),
                "email_envelope": self.email_envelope.get_as_dict(),
                "email_body": self.email_body.get_as_dict(),
                "email_attachment_collection": sorted([x.get_as_dict() for x in self.email_attachment_collection])
            }

    def write_avro(self,
                   avro_container_uri: str):
        # remember that the array of addreess_to_collection must go out as a list to be serialized by the python
        #  library for Avro.  In all our comparison code for this class we always convert from frozenset  to list
        #  and sort for purposes of comparison
        #
        #  We serialize the immutablelist object to a list before writing it
        #                 "email_attachment_collection": sorted(self.email_attachment_collection),
        # Each compound object needs to be serialized before recording
        #
        type(self)._write_avro_data(self,
                                    data_as_dictionary=self.get_as_dict(),
                                    avro_container_uri=avro_container_uri)

    @staticmethod
    def from_avro_as_dict(avro_parameter_dict: Dict):
        # we have a list of dictionaries for the email attachment, so we need to initialize each one before
        #  making a set
        email_attachment_list = []
        for this_email_attach_as_dict in avro_parameter_dict['email_attachment_collection']:
            email_attachment = EmailAttachment.from_avro_as_dict(avro_parameter_dict=this_email_attach_as_dict)
            print('The email attachment = ' + os.linesep + str(email_attachment))
            email_attachment_list.append(email_attachment)

        print('The email attachment list = ' + str(email_attachment_list))

        try:
            new_email_container = \
                EmailContainer(
                    container_parameters=
                    EmailContainerParameters(
                        email_message_metadata=
                        EmailMessageMetadata.from_avro_as_dict(
                            avro_parameter_dict=avro_parameter_dict['email_message_metadata']),
                        email_envelope=
                        EmailEnvelope.from_avro_as_dict(avro_parameter_dict=avro_parameter_dict['email_envelope']),
                        email_body=EmailBody.from_avro_as_dict(avro_parameter_dict=avro_parameter_dict['email_body']),
                        email_attachment_collection=frozenset(email_attachment_list)
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
        return new_email_container

    @staticmethod
    def from_avro(avro_container_uri: str):
        # pass up the exceptions
        datum_to_load = AbstractContainer._from_avro_generic(avro_container_uri=avro_container_uri)

        return EmailContainer.from_avro_as_dict(datum_to_load)

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
        super(EmailContainer, self).__init__(container_parameters=container_parameters)

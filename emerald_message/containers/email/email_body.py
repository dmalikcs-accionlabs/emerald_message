import os
from dataclasses import dataclass
from avro.datafile import DataFileWriter, DataFileException, DataFileReader
from avro.io import DatumReader, DatumWriter
from typing import Optional, List
from spooky import hash128

from emerald_message.containers.abstract_container import AbstractContainer, ContainerSchemaMatchingIdentifier
from emerald_message.avro_schemas.avro_message_schema_family import AvroMessageSchemaFamily

"""
Track the message contents in three ways:
The text field if empty will be initialized as an EMPTY string (len 0) vs being None
The HTML can be None to indicate a pure text email (i.e. if HTML is none)
1) message_body_html: Original HTML as received by email system
2) message_body_text: Text (without markup) as received and decoded by email system
3) message_body_stripped_lines: Text without line separators, organized into an ImmutableList

Use spooky hash v128 for the setup, as the message body could be quite large and we don't want collisions
"""


@dataclass(frozen=True)
class _EmailBodyParameters:
    message_body_text: str
    message_body_html: Optional[str] = None


class EmailBody(AbstractContainer):
    @property
    def message_body_text(self) -> str:
        return self._container_parameters.message_body_text

    @property
    def message_body_html(self):
        return self._container_parameters.message_body_html

    @property
    def message_body_as_lines_list(self) -> List[str]:
        # in this case the line terminations may not match the platform (i.e. may not be any conversion)
        #  so strip out any '\r' and then we will have lines to form list
        return [x for x in self.message_body_text.strip('\r').split('\n')]

    @classmethod
    def get_container_schema_matching_identifier(cls) -> ContainerSchemaMatchingIdentifier:
        return ContainerSchemaMatchingIdentifier(
            container_avro_schema_family_name=AvroMessageSchemaFamily.EMAIL,
            container_avro_schema_name='EmailBody'
        )

    def write_avro(self,
                   avro_container_uri: str):
        if type(avro_container_uri) is not str or len(avro_container_uri) == 0:
            raise ValueError('Unable to write avro - avro_container_uri parameter' +
                             ' must be a string specifying container location in writable form')
        if self.debug:
            print('Avro schema type = ' + str(type(type(self).get_avro_schema_record().avro_schema)))
            print('Avro schema = ' + str(type(self).get_avro_schema_record().avro_schema))

        writer = DataFileWriter(open(avro_container_uri, "wb"),
                                DatumWriter(),
                                type(self).get_avro_schema_record().avro_schema)
        if self.debug:
            print('Opened data file write, html type = ' + type(self.message_body_html).__name__)
        writer.append({"message_body_text": self.message_body_text,
                       "message_body_html": self.message_body_html})
        writer.close()

    @staticmethod
    def from_avro(avro_container_uri: str):
        reader = DataFileReader(open(avro_container_uri, "rb"), DatumReader())
        new_email_body = None
        for datum_counter, datum in enumerate(reader, start=1):
            print('Reading datum #' + str(datum_counter))
            print('The message datum = ' + str(datum))
            new_email_body = EmailBody(message_body_text=datum['message_body_text'],
                                       message_body_html=datum['message_body_html'])
        reader.close()
        if new_email_body is None:
            raise DataFileException('Data could not be loaded from AVRO file "' + str(avro_container_uri) +
                                    '" using schema ' + AbstractContainer.get_avro_schema_record().avro_schema_name)
        return new_email_body

    @property
    def length_html(self) -> int:
        return len(self.message_body_html)

    @property
    def length_text(self) -> int:
        return len(self.message_body_text)

    #
    #  Design note - we would like to use dataclasses because of additional parameters.  However, it is not
    #  possible to extend and use other variables in a dataclass if you set frozen to true
    #  Named Tuples have no such rules
    #
    def __init__(self,
                 message_body_text: str,
                 message_body_html: Optional[str] = None):
        super(EmailBody, self).__init__()
        # we want both immutable parameters and ability to extend this class from abstract
        #  as of 201908, only pattern DET sees is to store them in class as we would a named tuple
        #  otherwise we cannot set other instance parameters in here because without separate
        #  instance parameter "container_parameter" we'd end up setting dataclass to true on this actual class
        self._container_parameters = _EmailBodyParameters(message_body_text=message_body_text,
                                                          message_body_html=message_body_html)

        print('Message body initializing ' + str(self.__dict__))

    def __len__(self):
        return self.length_text

    def __str__(self):
        # the HTML rendering is considered the source if present, but this is to do true differencing so show all
        #  This is not the best way to print contents - just access members directly for that
        return 'EmailBody' + os.linesep + \
               'HTML:' + os.linesep + \
               (self.message_body_html if self.message_body_html is not None else 'NOT PROVIDED') + \
               os.linesep + \
               'Text:' + os.linesep + self.message_body_text + os.linesep + \
               'Stripped line(s) (count=' + str(len(self.message_body_as_lines_list)) + ')' + os.linesep + \
               os.linesep.join(str(x) for x in self.message_body_as_lines_list)

    def __hash__(self):
        # if there is no html, omit from the calculation as you can't take hash of None
        # the stripped
        return hash128(self.message_body_text).update(self.message_body_html) \
            if self.message_body_html is not None \
            else hash128(self.message_body_text)

    def __eq__(self, other):
        if not isinstance(other, EmailBody):
            return False

        # use spooky hash test here for consistent use of comparisons - body can be quite large
        if hash128(self.message_body_text) != hash128(other.message_body_text):
            return False

        if hash128(self.message_body_html) != hash128(other.message_body_html):
            return False

        return True

    def __ne__(self, other):
        return not (__eq__(self, other))

    def __lt__(self, other):
        #  the LT method is needed for sorting message body objects.  In reality, as text blobs
        #  we should be able to check just one of these three properties since all ultimately arise
        # from html.  Our convention is as follows:
        #  If the HTML element is EMPTY or NULL for SELF:
        #       IF HTML is EMPTY or NULL for OTHER:
        #           LT = False
        #       ELSE
        #           LT = False
        #  Else
        #       IF HTML element is empty or null for OTHER:
        #           LT = False
        #       ELSE
        #           Straight comparison of self.html < other.html
        #
        if not isinstance(other, EmailBody):
            raise TypeError('Cannot compare other object of type "' + str(type(other)) + ' to element of ' +
                            type(self).__name__)

        if self.message_body_html is None or len(self.message_body_html) == 0:
            if other.message_body_html is None or len(other.message_body_html) == 0:
                # covers case where we default to text comparison
                return self.message_body_text < other.message_body_text
            else:
                # other has HTML so we will treat as GT (i.e. self LT is true)
                return True
        else:
            if other.message_body_html is None or len(other.message_body_html) == 0:
                return False
            else:
                return self.message_body_text < other.message_body_text

        # suppressing pep warning because I think it is better as a reminder that all paths must exit before
        #  reaching this point
        # noinspection PyUnreachableCode
        raise AssertionError('Unexpected endpoint reached in comparison for type "' + EmailBody.__name__ + '"')

    def __gt__(self, other):
        #  the GT method is not really needed for sorting, but included so others can do their own comparisons
        #  See notes on __lt__
        if not isinstance(other, EmailBody):
            raise TypeError('Cannot compare other object of type "' + str(type(other)) + ' to element of ' +
                            type(self).__name__)

        if self.message_body_html is None or len(self.message_body_html) == 0:
            if other.message_body_html is None or len(other.message_body_html) == 0:
                # covers case where we default to text comparison
                return self.message_body_text > other.message_body_text
            else:
                # other has HTML so we will treat as GT (i.e. self LT is true)
                return False
        else:
            if other.message_body_html is None or len(other.message_body_html) == 0:
                return True
            else:
                return self.message_body_text > other.message_body_text

        # suppressing pep warning because I think it is better as a reminder that all paths must exit before
        #  reaching this point
        # noinspection PyUnreachableCode
        raise AssertionError('Unexpected endpoint reached in comparison for type "' + EmailBody.__name__ + '"')

    def __ge__(self, other):
        return not (__lt__(self, other))

    def __le__(self, other):
        return not (__gt__(self, other))

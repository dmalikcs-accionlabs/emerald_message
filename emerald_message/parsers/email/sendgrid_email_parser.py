import os
import json
import mimetypes
import datetime
import base64

from typing import List, Optional, FrozenSet
from flask import request
from werkzeug.local import LocalProxy
from werkzeug.utils import secure_filename
from werkzeug.datastructures import ImmutableList
from six import iteritems
from io import StringIO

from emerald_message.containers.email.email_container import EmailContainer
from emerald_message.containers.email.email_envelope import EmailEnvelope
from emerald_message.containers.email.email_body import EmailBody
from emerald_message.containers.email.email_message_metadata import EmailMessageMetadata
from emerald_message.containers.email.email_attachment import EmailAttachment

from emerald_message.logging.logger import EmeraldLogger

from emerald_message.error import EmeraldEmailParsingError


class ParsedEmail:
    @property
    def sendgrid_payload(self):
        return self._sendgrid_payload

    @property
    def logger(self) -> EmeraldLogger:
        return self._logger

    @property
    def email_container(self) -> EmailContainer:
        return self._email_container

    # now make elements available directly so callers do not have to load all the email
    #  container classes
    @property
    def address_from(self) -> str:
        return self._email_container.email_envelope.address_from

    @property
    def address_to_collection(self) -> FrozenSet[str]:
        return self._email_container.email_envelope.address_to_collection

    @property
    def subject_text(self) -> str:
        return self._email_container.email_envelope.message_subject

    @property
    def message_body_text(self) -> str:
        return self._email_container.email_body.message_body_text

    @property
    def message_body_html(self) -> Optional[str]:
        return self._email_container.email_body.message_body_html


    def __init__(self,
                 inbound_request: LocalProxy):
        inbound_request.get_data(as_text=True)

        self._logger = EmeraldLogger(logging_module_name=type(self).__name__)

        print('The type of request is ' + str(type(inbound_request)))
        self._sendgrid_payload = inbound_request.form
        print('the type of payload is ' + str(type(self._sendgrid_payload)))

        # The type of request is <class 'werkzeug.local.LocalProxy'>
        # the type of payload is <class 'werkzeug.datastructures.ImmutableMultiDict'>

        # run through the tuples that represent key value pairs
        email_data_dictionary = dict()
        for kvcount, kvpair in enumerate(self.sendgrid_payload.items(), start=1):
            print('Pair #' + str(kvcount) + ': ' + str(kvpair))
            email_data_dictionary[kvpair[0]] = kvpair[1]

        # get the charsets
        #   ('charsets', '{"to":"UTF-8","html":"UTF-8","subject":"UTF-8","from":"UTF-8","text":"UTF-8"}')
        try:
            self._charsets = self.sendgrid_payload['charsets']
        except KeyError:
            raise EmeraldEmailParsingError('Unable to find charsets in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))

        ###############
        #  Email Container Element: METADATA
        ###############

        #  DET FIXME TODO - get DKIM and SPF
        #   ('dkim', '{david@cottonfields.us : pass}')
        #   ('SPF', 'pass')
        #

        # get the headers
        try:
            email_message_headers = self.sendgrid_payload['headers']
        except KeyError:
            raise EmeraldEmailParsingError('Unable to find email headers in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))

        # get the sender ip
        #   ('sender_ip', '136.143.188.19')
        try:
            sender_ip = self.sendgrid_payload['sender_ip']
        except KeyError:
            raise EmeraldEmailParsingError('Unable to find sender_ip in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))

        # get attachment count and then we can decide if attachment parsing needed
        #       ('attachments', '0')
        try:
            attachment_count = int(self.sendgrid_payload['attachments'])
        except KeyError:
            raise EmeraldEmailParsingError('Unable to find attachments (the count) in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))
        except (TypeError, ValueError):
            raise EmeraldEmailParsingError('Unable to parse the attachment count as integer' +
                                           os.linesep + 'Value sent is ' + str(self.sendgrid_payload['attachments']))

        #
        #  SPF is a single check but we get as a string "pass"
        #
        try:
            email_spf_passed: Optional[bool] = True if self.sendgrid_payload['spf'].casefold() == 'pass' else False
        except KeyError:
            email_spf_passed: Optional[bool] = None

        #
        #  DKIM arrives as a dictionary keyed by email address - set TRUE only if all say pass
        #
        email_dkim_passed: Optional[bool] = None
        try:
            email_dkim_passed_dict_string: str = self.sendgrid_payload['dkim']
            #  unfortunately it comes like this: "{ myemail@mydomain.com: pass}" which because there are no quotes
            #  means you can't read with json.loads.  We need JSON.stringify as in javascript
            #  Just split the string on commas, then colons
            if not email_dkim_passed_dict_string.startswith('{'):
                raise ValueError('Expected DKIM string to begin with { character')
            if not email_dkim_passed_dict_string.endswith('}'):
                raise ValueError('Expected DKIM string to end with } character')
            # now strip braces and leading and trailing spaces
            email_dkim_passed_dict_string = email_dkim_passed_dict_string.lstrip('{').rstrip('}').lstrip().rstrip()
            email_dkim_entries = email_dkim_passed_dict_string.split(',')
            for entry_counter, this_entry in enumerate(email_dkim_entries, start=1):
                # split on colon to get key value and throw error if not two elements
                this_entry_kv_pair = this_entry.split(':')
                if len(this_entry_kv_pair) != 2:
                    raise ValueError('DKIM entry #' + str(entry_counter) + ' is not valid - expected format ' +
                                     '<key>:<value>' + os.linesep + '\tValue provided = ' +
                                     email_dkim_passed_dict_string)
                dkim_status_this_entry_string = this_entry_kv_pair[1].rstrip().lstrip().casefold()
                if dkim_status_this_entry_string != 'pass':
                    print('DKIM entry #' + str(entry_counter) + ' does not have pass value ' +
                          '<key>:<value>' + os.linesep + '\tValue provided = ' +
                          email_dkim_passed_dict_string + os.linesep +
                          '\tValue = ' + dkim_status_this_entry_string)
                    email_dkim_passed = False
                    break

            email_dkim_passed: Optional[bool] = True
        except KeyError:
            # Not found
            print('No DKIM status info passed for email')
            pass
        except ValueError as vex:
            print('Unable to parse expected pseudo-JSON DKIM dictionary - value is "' + str(
                self.sendgrid_payload['dkim']) +
                  '"' + os.linesep + 'Exception: ' + str(vex))
            pass

        email_container_metadata = EmailMessageMetadata(
            router_source_tag='self',
            routed_timestamp_iso8601=EmeraldLogger.get_iso8601_utc_now_string(),
            email_sender_ip=sender_ip,
            attachment_count=attachment_count,
            email_headers=email_message_headers,
            email_spf_sender_passed=email_spf_passed,
            email_dkim_sender_passed=email_dkim_passed
        )

        ###############
        #  Email Container Element: ENVELOPE
        ###############

        # read the envelope and get the to and from data more directly (we don't care about text name)
        #   ('to', '"test1" <test1@ingestion.dynastyse.com>')
        #   ('from', 'David Thompson <david@cottonfields.us>')
        #   ('envelope', '{"to":["test1@ingestion.dynastyse.com"],"from":"david@cottonfields.us"}')
        try:
            envelope_json = self.sendgrid_payload['envelope']
        except KeyError:
            raise EmeraldEmailParsingError('Unable to find envelope in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))

        #
        # Subject is in text form
        #   ('subject', 'with at')
        #
        try:
            email_subject = self.sendgrid_payload['subject']
        except KeyError:
            raise EmeraldEmailParsingError('Unable to parse subject in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))

        # SendGrid envelop contains only the from and to - we add subject
        try:
            envelope = json.loads(envelope_json)
        except json.JSONDecodeError as jdex:
            raise EmeraldEmailParsingError('Unable to parse expected data from envelope json string' +
                                           os.linesep + 'Value of text = ' + os.linesep +
                                           str(envelope_json) + os.linesep +
                                           'Exception info: ' + str(jdex))
        else:
            #
            #  Now initialize the email envelop
            #
            email_container_envelope = EmailEnvelope(
                address_from=envelope['from'],
                address_to_collection=frozenset(envelope['to']),
                message_subject=email_subject,
                message_rx_timestamp_iso8601=EmeraldLogger.get_iso8601_utc_now_string())

        ###############
        #  Email Container Element: BODY
        ###############

        # read the text of message, recognizing it could be in HTML form or even RTF
        #  Use the charsets to know the encoding
        #
        #  ('text', '')
        try:
            message_body_text = self.sendgrid_payload['text']
        except KeyError:
            raise EmeraldEmailParsingError('Unable to find text (message body) in key list for inbound email' +
                                           os.linesep + 'Keys found: ' +
                                           ','.join([str(x) for x in email_data_dictionary.keys()]))
        # and look for html as contents are there
        # Pair #4: ('html', '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
        # <html><head><meta content="text/html;charset=UTF-8" http-equiv="Content-Type"></head>
        # <body >
        # <div style="font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 10pt;">
        # <div>Hello this is <span class="highlight" style="background-color:#ff0000">html</span><br></div>
        # <div><br></div><div class="align-left" style="text-align: left;">Because<u> I marked it up</u></div><
        # /div><br></body></html>')

        try:
            message_body_html = self.sendgrid_payload['html']
        except KeyError:
            print('No HTML element in payload')
            message_body_html = None

        # DET TODO handle HTML and text encoding cases later
        email_container_body = EmailBody(message_body_text=message_body_text,
                                         message_body_html=message_body_html)

        ###############
        #  Email Container Element: ATTACHMENT COLLECTION
        ###############

        # if attachment count > 0 we need attachment-info
        attachments: List[EmailAttachment] = []
        if attachment_count > 0:
            print('Attachment count: ' + str(attachment_count))

            try:
                attachment_info_json = self.sendgrid_payload['attachment-info']
            except KeyError:
                raise EmeraldEmailParsingError('Unable to find attachment-info in key list for inbound email' +
                                               os.linesep + 'Attachment count shows as ' + str(attachment_count) +
                                               os.linesep + 'Keys found: ' +
                                               ','.join([str(x) for x in email_data_dictionary.keys()]))
            try:
                attachment_info = json.loads(attachment_info_json)
            except json.JSONDecodeError as jdex:
                raise EmeraldEmailParsingError('Unable to parse expected data from attachment-info json string' +
                                               os.linesep + 'Value of text = ' + os.linesep +
                                               str(attachment_info_json) + os.linesep)

            self.logger.logger.info('Attachment info: ' + str(attachment_info))

            print('Now get attachments - files type = ' + str(type(request.files)))
            # Now get attachments - files type = <class 'werkzeug.datastructures.ImmutableMultiDict'>

            for _, filestorage in iteritems(request.files):
                if filestorage.filename not in (None, 'fdopen', '<fdopen>'):
                    filename = secure_filename(filestorage.filename)
                    attachment = EmailAttachment(
                        filename=filename,
                        mimetype=filestorage.content_type,
                        contents_base64=base64.b64encode(filestorage.read())
                    )
                    attachments.append(attachment)
                else:
                    print('Found attachment filename as unsupported value "' + str(filestorage.filename))

            print('Total attachment count = ' + str(len(attachments)))
            print('Total specified count = ' + str(attachment_count))

        # Now build overall container
        self._email_container = EmailContainer(
            email_container_metadata=email_container_metadata,
            email_envelope=email_container_envelope,
            email_body=email_container_body,
            email_attachment_collection=ImmutableList(attachments)
        )

        print('Info on the email: ' + os.linesep + self._email_container.get_info())
        return

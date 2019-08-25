import argparse
import os
import platform
import sys
import datetime
import pkg_resources

from pytz import timezone
from netaddr import IPAddress
from werkzeug.datastructures import ImmutableList

from emerald_message.exitcode import ExitCode
from emerald_message.error import EmeraldMessageContainerInitializationError
from emerald_message.logging.logger import EmeraldLogger
from emerald_message.version import __version__
from emerald_message.avro_schemas.avro_message_schemas import AvroMessageSchemaFrozen
from emerald_message.containers.email.email_body import EmailBody, EmailBodyParameters
from emerald_message.containers.email.email_attachment import EmailAttachment, EmailAttachmentParameters
from emerald_message.containers.email.email_envelope import EmailEnvelope, EmailEnvelopeParameters
from emerald_message.containers.email.email_message_metadata import EmailMessageMetadata, EmailMessageMetadataParameters
from emerald_message.containers.email.email_container import EmailContainer, EmailContainerParameters

MIN_PYTHON_VER_MAJOR = 3
MIN_PYTHON_VER_MINOR = 7


def get_command_info_as_string() -> str:
    the_package = __name__
    the_path = '/'.join(('reference', 'command_notes.txt'))
    the_reference_info = pkg_resources.resource_string(the_package, the_path).decode('utf-8')

    return the_reference_info


def emerald_message_launcher(argv):
    try:
        appname = __name__.split('.')[0]
    except KeyError:
        appname = __name__

    if sys.version_info.major < MIN_PYTHON_VER_MAJOR or (
            sys.version_info.major == MIN_PYTHON_VER_MAJOR and sys.version_info.minor < MIN_PYTHON_VER_MINOR):
        print('Unable to run this script - python interpreter must be at or above version ' +
              str(MIN_PYTHON_VER_MAJOR) + '.' + str(MIN_PYTHON_VER_MINOR) +
              os.linesep + '\tVersion is ' + str(sys.version_info.major) + '.' + str(sys.version_info.minor))
        return ExitCode.PythonVersionError
    else:
        print('Running on python interpreter version ' +
              str(sys.version_info.major) + '.' + str(sys.version_info.minor))
        print('\tPlatform: ' + platform.platform() + os.linesep +
              '\tMachine: ' + platform.machine())

    parser = argparse.ArgumentParser(prog=appname,
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     epilog=get_command_info_as_string())
    parser.add_argument('--version',
                        action='version',
                        version=__version__)
    parser.add_argument('--list_avro_schemas',
                        action='store_true',
                        help='Specify to list the supported AVRO schema(s)')
    parser.add_argument('--parse_avro',
                        action='store',
                        help='Parse an AVRO file using specific schema. Format is <datafilepath>:<schemaname>' +
                             os.linesep + 'TODO ADD ENUMERATION HERE')

    args = parser.parse_args(None if argv[0:] else ['--help'])

    logger = EmeraldLogger(logging_module_name='launcher')

    startup_time_utc = datetime.datetime.now(tz=timezone('UTC'))

    print('Args = ' + str(args.__dict__))
    logger.logger.warn('Initializing ' + appname + ' Version ' + __version__ + ', startup at ' +
                       datetime.datetime.strftime(startup_time_utc, '%Y%m%dT%H:%M:%S%z'))

    # Commands should not block for long periods of time - this is designed for simple requests
    #  Use the library from other packages to implement long running tasks

    if args.list_avro_schemas is True:
        logger.logger.info('Running list avro schema_email')

        # random testing EmailBody
        try:
            the_email_body = \
                EmailBody(container_parameters=
                          EmailBodyParameters(message_body_text='Hello World EVEN MORE',
                                              message_body_html='GOGOGO'))
        except EmeraldMessageContainerInitializationError as mex:
            logger.logger.critical('Unable to initialize email test object' + os.linesep +
                                   'Error details: ' + str(mex.args))
            return ExitCode.CodeError
        logger.logger.info('email body test = ' + os.linesep + str(the_email_body) + os.linesep)
        logger.logger.info('AVRO schema namespaces: ' + os.linesep +
                           os.linesep.join([x for x in sorted(
                               AvroMessageSchemaFrozen.avro_schema_collection.avro_schema_namespaces)]))
        # now write
        the_email_body.write_avro('/Users/davidthompson/Documents/hello.avro')

        # now read back
        try:
            new_version = EmailBody.from_avro('/Users/davidthompson/Documents/hello.avro')
        except FileNotFoundError as fex:
            logger.logger.critical('Unable to locate avro object for reading' + os.linesep +
                                   'Error details: ' + str(fex.args))
            return ExitCode.CodeError
        else:
            print('The new version = ' + str(new_version))
            print('Compare test for body = ' + str(new_version == the_email_body))
            print(os.linesep)

        # again  EmailEnvelope
        # random testing
        try:
            the_email_envelope = \
                EmailEnvelope(container_parameters=EmailEnvelopeParameters(
                    address_from='det@go.com',
                    address_to_collection=frozenset(['gogetem@dynastyse.com', 'another@gogo.com']),
                    message_subject='Big Message',
                    message_rx_timestamp_iso8601=
                    datetime.datetime.strftime(startup_time_utc, '%Y%m%dT%H:%M:%S%z')
                ))
        except EmeraldMessageContainerInitializationError as mex:
            logger.logger.critical('Unable to initialize email test envelope  object' + os.linesep +
                                   'Error details: ' + str(mex.args[0]))
            return ExitCode.CodeError
        logger.logger.info('email envelope test = ' + os.linesep + str(the_email_envelope) + os.linesep)
        logger.logger.info('AVRO schema namespaces: ' + os.linesep +
                           os.linesep.join([x for x in sorted(
                               AvroMessageSchemaFrozen.avro_schema_collection.avro_schema_namespaces)]))
        # now write
        output_fn = '/Users/davidthompson/Documents/hello_envelope.avro'
        the_email_envelope.write_avro(output_fn)

        # now read back
        try:
            new_version_envelope = EmailEnvelope.from_avro(output_fn)
        except FileNotFoundError as fex:
            logger.logger.critical('Unable to locate avro object for reading' + os.linesep +
                                   'Error details: ' + str(fex.args))
            return ExitCode.CodeError
        else:
            print('The new version = ' + str(new_version_envelope))
            print('Compare test for envelope = ' + str(new_version_envelope == the_email_envelope))
            print(os.linesep)

        # again EmailMessageMetadata
        # random testing
        try:
            the_email_metadata = \
                EmailMessageMetadata(container_parameters=EmailMessageMetadataParameters(
                    router_source_tag='Blue',
                    routed_timestamp_iso8601=datetime.datetime.strftime(startup_time_utc, '%Y%m%dT%H:%M:%S%z'),
                    email_sender_ip=
                    IPAddress('10.10.1.1'),
                    attachment_count=1,
                    email_headers='Extra headers',
                    email_spf_sender_passed=True,
                    email_dkim_sender_passed=None
                ))
        except EmeraldMessageContainerInitializationError as mex:
            logger.logger.critical('Unable to initialize email test metadata  object' + os.linesep +
                                   'Error details: ' + str(mex.args[0]))
            return ExitCode.CodeError
        logger.logger.info('email metadata test = ' + os.linesep + str(the_email_metadata) + os.linesep)
        logger.logger.info('AVRO schema namespaces: ' + os.linesep +
                           os.linesep.join([x for x in sorted(
                               AvroMessageSchemaFrozen.avro_schema_collection.avro_schema_namespaces)]))
        # now write
        output_fn = '/Users/davidthompson/Documents/hello_metadata.avro'
        the_email_metadata.write_avro(output_fn)

        # now read back
        try:
            new_version_metadata = EmailMessageMetadata.from_avro(output_fn)
        except FileNotFoundError as fex:
            logger.logger.critical('Unable to locate avro object for reading' + os.linesep +
                                   'Error details: ' + str(fex.args))
            return ExitCode.CodeError
        else:
            print('The new version = ' + str(new_version_metadata))
            print('Compare test for metadata = ' + str(new_version_metadata == the_email_metadata))
            print(os.linesep)

        # again EmailAttachment
        # random testing
        try:
            the_email_attachment = \
                EmailAttachment(container_parameters=EmailAttachmentParameters(
                    filename='HelloFile',
                    mimetype='me',
                    contents_base64=
                    EmailAttachment.transform_base_64_encode(element='teststring')
                ))
        except EmeraldMessageContainerInitializationError as mex:
            logger.logger.critical('Unable to initialize email test attach  object' + os.linesep +
                                   'Error details: ' + str(mex.args[0]))
            return ExitCode.CodeError
        logger.logger.info('email attach test = ' + os.linesep + str(the_email_attachment) + os.linesep)
        logger.logger.info('AVRO schema namespaces: ' + os.linesep +
                           os.linesep.join([x for x in sorted(
                               AvroMessageSchemaFrozen.avro_schema_collection.avro_schema_namespaces)]))
        # now write
        output_fn = '/Users/davidthompson/Documents/hello_attach.avro'
        the_email_attachment.write_avro(output_fn)

        # now read back
        try:
            new_version_attach = EmailAttachment.from_avro(output_fn)
        except FileNotFoundError as fex:
            logger.logger.critical('Unable to locate avro object for reading' + os.linesep +
                                   'Error details: ' + str(fex.args))
            return ExitCode.CodeError
        else:
            print('The new version = ' + str(new_version_attach))
            print('Compare test for attachmment = ' + str(new_version_attach == the_email_attachment))
            print(os.linesep)

        # again EmailContainer
        # random testing
        try:
            the_email_container = \
                EmailContainer(container_parameters=EmailContainerParameters(
                    email_message_metadata=the_email_metadata,
                    email_envelope=the_email_envelope,
                    email_body=the_email_body,
                    email_attachment_collection=frozenset([the_email_attachment])
                ))
        except EmeraldMessageContainerInitializationError as mex:
            logger.logger.critical('Unable to initialize email test container  object' + os.linesep +
                                   'Error details: ' + str(mex.args[0]))
            return ExitCode.CodeError
        logger.logger.info('email container test = ' + os.linesep + str(the_email_container) + os.linesep)
#        logger.logger.info('email attach test attach len = ' +
#                           str(len(the_email_container.email_attachment_collection)))
        logger.logger.info('AVRO schema namespaces: ' + os.linesep +
                           os.linesep.join([x for x in sorted(
                               AvroMessageSchemaFrozen.avro_schema_collection.avro_schema_namespaces)]))
        # now write
        output_fn = '/Users/davidthompson/Documents/hello_container.avro'
        the_email_container.write_avro(output_fn)

        # now read back
        logger.logger.info('Now reading container from ' + output_fn)
        try:
            new_version_container = EmailContainer.from_avro(output_fn)
        except FileNotFoundError as fex:
            logger.logger.critical('Unable to locate avro object for reading' + os.linesep +
                                   'Error details: ' + str(fex.args))
            return ExitCode.CodeError
        else:
            print('The orig version = ' + os.linesep + str(the_email_container) + os.linesep +
                  str(hash(the_email_container)) + os.linesep)
            print('The new version = ' + os.linesep + str(new_version_container) + os.linesep +
                  str(hash(new_version_container)) + os.linesep)
            print('Compare test for container = ' + str(new_version_container == the_email_container))

        return ExitCode.Success

    return ExitCode.Success

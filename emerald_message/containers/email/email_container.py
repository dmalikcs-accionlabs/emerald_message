import os

from typing import NamedTuple
from werkzeug.datastructures import ImmutableList

from emerald_message.containers.email.email_attachment import EmailAttachment
from emerald_message.containers.email.email_body import EmailBody
from emerald_message.containers.email.email_envelope import EmailEnvelope
from emerald_message.containers.email.email_message_metadata import EmailMessageMetadata


# Since order of the attachments may matter, store as an immutable list
#  You can't use type hints to say ImmutableList[EmailAttachment]

class EmailContainer(NamedTuple):
    email_container_metadata: EmailMessageMetadata
    email_envelope: EmailEnvelope
    email_body: EmailBody
    email_attachment_collection: ImmutableList

    def get_info(self) -> str:
        return os.linesep.join([
            'Email Envelope: ' + str(self.email_envelope),
            'Email Body Length: ' + str(len(self.email_body)),
            'Email Attachment Count: ' + str(len(self.email_attachment_collection))
        ])

import os
from typing import NamedTuple, Optional

from netaddr import IPAddress


class EmailMessageMetadata(NamedTuple):
    router_source_tag: str
    routed_timestamp_iso8601: str
    email_sender_ip: IPAddress
    attachment_count: int
    email_headers: str
    email_spf_sender_passed: Optional[bool] = None
    email_dkim_sender_passed: Optional[bool] = None

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
        return not (__eq__(self, other))

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

    def __le__(self, other):
        return not (__gt__(self, other))

    def __ge__(self, other):
        return not (__lt__(self, other))

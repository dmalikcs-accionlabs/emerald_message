import os
from typing import NamedTuple, FrozenSet


class EmailEnvelope(NamedTuple):
    address_from: str
    address_to_collection: FrozenSet[str]
    message_subject: str
    message_rx_timestamp_iso8601: str

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

        if self.address_to_collection != other.address_to_collection:
            return False

        if self.message_rx_timestamp_iso8601 != other.message_rx_timestamp_iso8601:
            return False

        return True

    def __ne__(self, other):
        return not (__eq__(self, other))

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
    return not (__lt__(self, other))


def __le__(self, other):
    return not (__gt__(self, other))

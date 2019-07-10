import os
from typing import NamedTuple
from spooky import hash128


class EmailAttachment(NamedTuple):
    filename: str
    mimetype: str
    contents_base64: str

    def __str__(self):
        return 'Filename: "' + str(self.filename) + '"' + os.linesep + \
               'Mimetype: "' + str(self.mimetype) + '"' + os.linesep + \
               'Content length: "' + str(len(self.contents_base64)) + '"' + os.linesep + \
               'Content hash128"' + hash128(self.contents_base64).hexdigest() + '"'

    # use spooky hash in 128 bit length as the attachments could be quite large - no collisions!
    def __hash__(self):
        return hash128(self.filename).update(self.mimetype).update(self.contents_base64)

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
        if hash_self < other.self:
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
        if hash_self > other.self:
            return True
        elif hash_self < hash_other:
            return False

        # getting here means they are equal
        return False

    def __le__(self, other):
        return not(__gt__(self, other))

    def __ge__(self, other):
        return not(__lt__(self, other))

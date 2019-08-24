import avro.schema
import json
from abc import ABCMeta, abstractmethod

from emerald_message.avro_schemas.avro_message_schemas import AvroMessageSchemas

class AbstractContainer(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def get_avro_schema_filename(cls) -> str:
        pass

    @classmethod
    def get_avro_schema(cls) -> avro.schema.RecordSchema:
        # all of the avro_schemas are stored in a package directory called avro_schemas (we use blank __init__.py to
        #  allow us to do relative imports
        #  we use MANIFEST.in and setup.py config to ensure these get pulled into the distribution files
        #  Find the schema file name identified by the implementing class' schema filename
        #
        #  DET NOTE:
        #  Use twisted's getModule to find the files as it is a bit more tolerant of zip vs directory packages
        #  ALso note at present this assumes the class making the request is in a SUBFOLDEER relative
        #  to this.  One could do comparisons on the filepath and adjust the number of pareent() calls if needed
        #
        avro_schema_filename = cls.get_avro_schema_filename()
        print('Using avro filename = ' + avro_schema_filename)
        if type(avro_schema_filename) is not str or len(avro_schema_filename) == 0:
            raise EnvironmentError('Unable to locate the required avro schema filename for class "' +
                                   cls.__name__ + '" - unable to proceed')
        return \
            avro.schema.Parse(
                json.dumps(
                    json.load(
                        pkg_resources.open_text(avro_schemas, avro_schema_filename)
                    )
                )
            )

    @abstractmethod
    def write_avro(self):
        pass

    @staticmethod
    @abstractmethod
    def from_avro(avro_container_uri: str):
        pass

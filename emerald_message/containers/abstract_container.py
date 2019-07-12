import avro.schema
import json
from twisted.python.modules import getModule

from abc import ABCMeta, abstractmethod


class AbstractContainer(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def get_avro_schema_filename(cls) -> str:
        pass

    @classmethod
    def get_avro_schema(cls) -> avro.schema.RecordSchema:
        # all of the schemas are stored in a non-python package directory called schemas
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
                        getModule(__name__).filePath.parent().parent().child('schemas').child(
                            avro_schema_filename).open(
                            mode='r')
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

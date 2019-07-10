import logging
import logging.handlers
import sys
import time
import datetime
from typing import Optional
from pytz import timezone


class EmeraldLogger:
    @classmethod
    def get_valid_logging_level(cls,
                                logging_level: int) -> int:
        allowed_levels = frozenset([logging.CRITICAL, logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG])
        return logging_level \
            if logging_level in allowed_levels \
            else logging.INFO

    @property
    def logger(self):
        return self._logger

    @staticmethod
    def get_iso8601_utc_now_string() -> str:
        return datetime.datetime.strftime(
            datetime.datetime.now(tz=timezone('UTC')),
            '%Y%m%dT%H:%M:%S%z')

    def __init__(self,
                 logging_module_name: str,
                 console_logging_level: Optional[int] = logging.INFO,
                 global_logging_level: Optional[int] = logging.INFO,
                 propagate: bool = True,
                 logging_module_description: Optional[str] = None,
                 use_localtimezone: bool = False):
        self._console_logging_level = type(self).get_valid_logging_level(console_logging_level)
        self._global_logging_level = type(self).get_valid_logging_level(global_logging_level)

        self._logging_module_description = logging_module_description \
            if logging_module_description is not None else logging_module_name

        self._logging_module_name = logging_module_name

        self._logger = logging.getLogger(name=self._logging_module_name)
        self._logger.propagate = propagate
        self._logger.setLevel(self._global_logging_level)

        # build console logger
        logger_console_handler = logging.StreamHandler(stream=sys.stdout)

        logger_console_fmt = logging.Formatter(fmt='%(asctime)s::%(name)s::%(levelname)s::%(message)s',
                                               datefmt='%Y-%m-%d:%H:%M:%S%z',
                                               style='%')
        # use UTC by default
        logger_console_fmt.converter = time.localtime if use_localtimezone else time.gmtime

        logger_console_handler.setLevel(self._console_logging_level)
        logger_console_handler.setFormatter(logger_console_fmt)

        self._logger.addHandler(logger_console_handler)

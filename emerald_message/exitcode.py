from enum import IntEnum, unique


@unique
class ExitCode(IntEnum):
    Success = 0
    HelpOrVersionOnly = -1
    PythonVersionError = -2

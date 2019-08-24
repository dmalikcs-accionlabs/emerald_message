from enum import IntEnum, unique


@unique
class ExitCode(IntEnum):
    Success = 0
    HelpOrVersionOnly = -1
    PythonVersionError = -2
    ArgumentError = -3
    CodeError = -4
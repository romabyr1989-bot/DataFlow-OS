class DataFlowError(Exception):
    pass


class AuthError(DataFlowError):
    pass


class PermissionError(DataFlowError):
    pass


class TableNotFoundError(DataFlowError):
    pass

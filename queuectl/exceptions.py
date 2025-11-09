class QueueCTLError(Exception):
    pass


class JobValidationError(QueueCTLError):
    pass


class JobNotFoundError(QueueCTLError):
    pass


class WorkerError(QueueCTLError):
    pass


class DatabaseError(QueueCTLError):
    pass


class ConfigurationError(QueueCTLError):
    pass
"""Classes representing errors relating to invalid queries, etc."""


class CensusAPIUnknownError(RuntimeError):
    """Exception representing an unknown error from the Census API."""


class InvalidGeographyError(ValueError):
    """Exception representing invalid geography combinations."""


class InvalidYearError(ValueError):
    """Exception representing one or more invalid years."""


class InvalidVariableError(ValueError):
    """Exception representing one or more invalid variables."""


class MissingCredentialsError(RuntimeError):
    """Exception indicating that API credentials are unavailable."""

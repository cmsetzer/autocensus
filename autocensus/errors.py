"""Classes representing errors relating to invalid queries, etc."""


class CensusAPIUnknownError(RuntimeError):
    """Exception representing an unknown error from the Census API."""
    pass


class InvalidQueryError(ValueError):
    """Exception representing one or more invalid query parameters."""
    pass


class MissingCredentialsError(RuntimeError):
    """Exception indicating that Socrata credentials are unavailable."""
    pass

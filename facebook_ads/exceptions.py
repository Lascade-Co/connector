from dlt.extract.exceptions import DltResourceException


class InsightsJobTimeout(DltResourceException):
    pass


class InsightsJobFailed(DltResourceException):
    """Raised when Meta reports a terminal failed or skipped async job."""

    def __init__(
        self,
        resource_name: str,
        msg: str,
        *,
        status: str,
        error_code: int | None = None,
        error_subcode: int | None = None,
    ) -> None:
        self.status = status
        self.error_code = error_code
        self.error_subcode = error_subcode
        super().__init__(resource_name, msg)

"""Shared test helpers: fixture locations and fake API errors."""

from pathlib import Path

import grpc
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v25.errors.types.errors import (
    ErrorCode,
    GoogleAdsError,
    GoogleAdsFailure,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "google_ads"
CUSTOMER_ID = "1234567890"


class FakeRpcError(grpc.RpcError):
    """What the client library raises for INTERNAL, RESOURCE_EXHAUSTED and transport errors."""

    def __init__(self, code: grpc.StatusCode) -> None:
        super().__init__(code.name)
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


def ads_exception(status: grpc.StatusCode, **error_code: object) -> GoogleAdsException:
    """A GoogleAdsException carrying one error, e.g. ads_exception(..., quota_error=...)."""
    failure = GoogleAdsFailure(
        errors=[GoogleAdsError(error_code=ErrorCode(**error_code), message="test error")]
    )
    return GoogleAdsException(FakeRpcError(status), None, failure, "test-request-id")


def load_script(name: str):
    """Import a file from scripts/ as a module."""
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses look their module up while being created
    spec.loader.exec_module(module)
    return module

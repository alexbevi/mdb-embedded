"""
Mongo-compatible error codes and response formatting for the wire layer.
"""

from ._types import ResponseDoc

_ERROR_CODES: dict[str, tuple[int, str]] = {
    "InternalError": (1, "InternalError"),
    "BadValue": (2, "BadValue"),
    "NoSuchKey": (4, "NoSuchKey"),
    "Unauthorized": (13, "Unauthorized"),
    "TypeMismatch": (14, "TypeMismatch"),
    "AuthenticationFailed": (18, "AuthenticationFailed"),
    "IllegalOperation": (20, "IllegalOperation"),
    "NamespaceNotFound": (26, "NamespaceNotFound"),
    "IndexNotFound": (27, "IndexNotFound"),
    "CursorNotFound": (43, "CursorNotFound"),
    "NamespaceExists": (48, "NamespaceExists"),
    "ExceededTimeLimit": (50, "ExceededTimeLimit"),
    "CommandNotFound": (59, "CommandNotFound"),
    "WriteConcernFailed": (64, "WriteConcernFailed"),
    "InvalidLength": (70, "InvalidLength"),
    "InvalidOptions": (72, "InvalidOptions"),
    "InvalidNamespace": (73, "InvalidNamespace"),
    "OperationFailed": (96, "OperationFailed"),
    "CommandNotSupported": (115, "CommandNotSupported"),
    "DuplicateKey": (11000, "DuplicateKey"),
    "DocumentValidationFailure": (121, "DocumentValidationFailure"),
    "NotPrimaryOrSecondary": (13436, "NotPrimaryOrSecondary"),
}


def error_response(code: int, code_name: str, message: str) -> ResponseDoc:
    """Build a standard Mongo error response document."""
    return {"ok": 0, "errmsg": str(message), "code": code, "codeName": code_name}


def make_error(name: str, message: str) -> ResponseDoc:
    """Build an error response by symbolic name (falls back to InternalError)."""
    code, code_name = _ERROR_CODES.get(name, (1, "InternalError"))
    return error_response(code, code_name, message)

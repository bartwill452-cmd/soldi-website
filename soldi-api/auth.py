from typing import Optional

from fastapi import HTTPException, Security
from fastapi.security import APIKeyQuery, APIKeyHeader

api_key_query = APIKeyQuery(name="apiKey", auto_error=False)
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)


def verify_api_key(
    expected_key: str,
    query_key: Optional[str] = None,
    header_key: Optional[str] = None,
) -> str:
    # Try query param first
    if query_key and query_key == expected_key:
        return query_key

    # Try Authorization header (strip "Bearer " prefix)
    if header_key:
        token = header_key.replace("Bearer ", "").strip()
        if token == expected_key:
            return token

    raise HTTPException(status_code=401, detail="Invalid or missing API key")

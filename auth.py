from fastapi import HTTPException, Security, Depends, Request
from fastapi.security import APIKeyHeader
from typing import Optional
import os

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
NODE_SECRET = os.getenv("NODE_SECRET", "node_secret")

async def verify_api_key(request: Request, api_key: Optional[str] = Security(api_key_header)):
    if request.headers.get("X-Node-Secret") == NODE_SECRET:
        return "internal"
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API Key")
    import database
    if database.db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    key_info = await database.db.get_api_key_info(api_key)
    if not key_info:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    allowed = await database.db.increment_usage(api_key)
    if not allowed:
        raise HTTPException(status_code=429, detail="Daily limit exceeded")
    return api_key

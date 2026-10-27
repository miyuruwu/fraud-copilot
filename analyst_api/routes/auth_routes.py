from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..auth import authenticate, create_token

router = APIRouter(tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/auth/login")
async def login(body: LoginRequest):
    user = authenticate(body.username, body.password)
    if not user:
        raise HTTPException(401, "Sai tài khoản hoặc mật khẩu")
    return {"access_token": create_token(user), "token_type": "bearer", "role": user.role}

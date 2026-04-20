"""Auth API routes — login and profile."""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.database import get_db
from ..models.user import User
from ..auth.security import (
    verify_password,
    create_access_token,
    get_current_user,
)
from ..auth.schemas import LoginRequest, TokenResponse, UserResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    """POST /api/auth/login — Authenticate and return JWT."""
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is deactivated")

    # Check demo expiry
    if user.is_demo and user.expires_at:
        if datetime.now(timezone.utc) > user.expires_at:
            raise HTTPException(status_code=403, detail="Demo account has expired")

    # Update last login
    user.last_login_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(user)

    token = create_access_token(str(user.id), user.role)
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


@router.get("/me", response_model=UserResponse)
async def get_me(user: User = Depends(get_current_user)):
    """GET /api/auth/me — Current user profile."""
    return UserResponse.model_validate(user)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(user: User = Depends(get_current_user)):
    """POST /api/auth/refresh — Issue a fresh JWT for the current session.

    Called silently by the client before long-running uploads or when the
    existing token is close to expiry, so a clinician who spent 30+ min
    filling in a case doesn't lose their work to a 401 on submit.
    """
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is deactivated")

    if user.is_demo and user.expires_at:
        if datetime.now(timezone.utc) > user.expires_at:
            raise HTTPException(status_code=403, detail="Demo account has expired")

    token = create_access_token(str(user.id), user.role)
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )

"""Auth Pydantic schemas."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr


class LoginRequest(BaseModel):
    email: str
    password: str


class UserResponse(BaseModel):
    id: UUID
    email: str
    full_name: str
    role: str
    is_demo: bool
    expires_at: Optional[datetime] = None
    max_reports: Optional[int] = None
    reports_used: int = 0
    is_active: bool = True
    last_login_at: Optional[datetime] = None
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class CreateDemoUserRequest(BaseModel):
    email: str
    password: str
    full_name: str
    expires_at: Optional[datetime] = None
    max_reports: Optional[int] = None
    notes: Optional[str] = None


class UpdateDemoUserRequest(BaseModel):
    full_name: Optional[str] = None
    expires_at: Optional[datetime] = None
    max_reports: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None
    reset_reports_used: Optional[bool] = None


class UserStatsResponse(UserResponse):
    cases_submitted: int = 0


class AdminStatsResponse(BaseModel):
    total_users: int
    active_demo_users: int
    total_cases: int
    total_reports: int

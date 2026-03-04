"""Admin API routes — user management and stats."""

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.database import get_db
from ..models.user import User
from ..models.case import Case
from ..models.report import Report
from ..auth.security import require_admin, hash_password
from ..auth.schemas import (
    CreateDemoUserRequest,
    UpdateDemoUserRequest,
    UserStatsResponse,
    AdminStatsResponse,
)

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/stats", response_model=AdminStatsResponse)
async def admin_stats(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/stats — Dashboard summary."""
    total_users = (await db.execute(
        select(func.count(User.id)).where(User.role != "admin")
    )).scalar() or 0

    active_demos = (await db.execute(
        select(func.count(User.id)).where(
            User.is_demo == True,
            User.is_active == True,
        )
    )).scalar() or 0

    total_cases = (await db.execute(select(func.count(Case.id)))).scalar() or 0
    total_reports = (await db.execute(select(func.count(Report.id)))).scalar() or 0

    return AdminStatsResponse(
        total_users=total_users,
        active_demo_users=active_demos,
        total_cases=total_cases,
        total_reports=total_reports,
    )


@router.get("/users", response_model=list[UserStatsResponse])
async def list_users(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/users — List all non-admin users with usage stats."""
    result = await db.execute(
        select(User).where(User.role != "admin").order_by(User.created_at.desc())
    )
    users = result.scalars().all()

    user_stats = []
    for u in users:
        count_result = await db.execute(
            select(func.count(Case.id)).where(Case.user_id == u.id)
        )
        cases_count = count_result.scalar() or 0
        user_data = UserStatsResponse.model_validate(u)
        user_data.cases_submitted = cases_count
        user_stats.append(user_data)

    return user_stats


@router.post("/users", status_code=201, response_model=UserStatsResponse)
async def create_user(
    body: CreateDemoUserRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """POST /api/admin/users — Create a demo user."""
    # Check for duplicate email
    existing = await db.execute(select(User).where(User.email == body.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        email=body.email,
        hashed_password=hash_password(body.password),
        full_name=body.full_name,
        role="user",
        is_demo=True,
        expires_at=body.expires_at,
        max_reports=body.max_reports,
        notes=body.notes,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    user_data = UserStatsResponse.model_validate(user)
    user_data.cases_submitted = 0
    return user_data


@router.patch("/users/{user_id}", response_model=UserStatsResponse)
async def update_user(
    user_id: UUID,
    body: UpdateDemoUserRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """PATCH /api/admin/users/{id} — Update user limits/status."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if body.full_name is not None:
        user.full_name = body.full_name
    if body.expires_at is not None:
        user.expires_at = body.expires_at
    if body.max_reports is not None:
        user.max_reports = body.max_reports
    if body.is_active is not None:
        user.is_active = body.is_active
    if body.notes is not None:
        user.notes = body.notes
    if body.reset_reports_used:
        user.reports_used = 0

    user.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(user)

    count_result = await db.execute(
        select(func.count(Case.id)).where(Case.user_id == user.id)
    )
    cases_count = count_result.scalar() or 0
    user_data = UserStatsResponse.model_validate(user)
    user_data.cases_submitted = cases_count
    return user_data


@router.delete("/users/{user_id}", status_code=200)
async def delete_user(
    user_id: UUID,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """DELETE /api/admin/users/{id} — Soft-delete (deactivate)."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_active = False
    user.updated_at = datetime.now(timezone.utc)
    await db.commit()

    return {"detail": "User deactivated"}

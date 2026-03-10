"""Admin API routes — user management, stats, and usage dashboard."""

from datetime import datetime, timezone, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, text
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


# ============================================================
# Usage Dashboard
# ============================================================

@router.get("/usage/summary")
async def usage_summary(
    days: int = Query(7, ge=1, le=90),
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/usage/summary — Aggregated usage stats.

    Returns per-service call counts, total cost, error rates, and
    average duration for the last N days.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)

    result = await db.execute(text("""
        SELECT
            service,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE status = 'success') AS success_count,
            COUNT(*) FILTER (WHERE status = 'error') AS error_count,
            ROUND(AVG(duration_ms)::numeric, 1) AS avg_duration_ms,
            COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
            COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
            COALESCE(SUM(input_chars), 0) AS total_input_chars,
            COALESCE(SUM(output_chars), 0) AS total_output_chars,
            ROUND(COALESCE(SUM(estimated_cost_usd), 0)::numeric, 6) AS total_cost_usd
        FROM usage_log
        WHERE timestamp >= :since
        GROUP BY service
        ORDER BY total_calls DESC
    """), {"since": since})

    rows = result.mappings().all()
    return {
        "period_days": days,
        "since": since.isoformat(),
        "services": [dict(r) for r in rows],
    }


@router.get("/usage/by-module")
async def usage_by_module(
    days: int = Query(7, ge=1, le=90),
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/usage/by-module — Usage broken down by module + service."""
    since = datetime.now(timezone.utc) - timedelta(days=days)

    result = await db.execute(text("""
        SELECT
            module, service, operation,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE status = 'error') AS errors,
            ROUND(AVG(duration_ms)::numeric, 1) AS avg_duration_ms,
            ROUND(COALESCE(SUM(estimated_cost_usd), 0)::numeric, 6) AS total_cost_usd
        FROM usage_log
        WHERE timestamp >= :since
        GROUP BY module, service, operation
        ORDER BY module, total_calls DESC
    """), {"since": since})

    rows = result.mappings().all()
    return {"period_days": days, "breakdown": [dict(r) for r in rows]}


@router.get("/usage/by-case/{case_id}")
async def usage_by_case(
    case_id: UUID,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/usage/by-case/{id} — Full audit trail for a single case/report."""
    result = await db.execute(text("""
        SELECT
            timestamp, module, service, operation,
            request_summary, model, status, error_message,
            duration_ms, input_tokens, output_tokens,
            input_chars, output_chars, num_results,
            estimated_cost_usd, metadata
        FROM usage_log
        WHERE case_id = CAST(:case_id AS uuid)
        ORDER BY timestamp ASC
    """), {"case_id": str(case_id)})

    rows = result.mappings().all()

    total_cost = sum(float(r.get("estimated_cost_usd") or 0) for r in rows)
    total_duration = sum(int(r.get("duration_ms") or 0) for r in rows)

    return {
        "case_id": str(case_id),
        "total_api_calls": len(rows),
        "total_cost_usd": round(total_cost, 6),
        "total_duration_ms": total_duration,
        "calls": [dict(r) for r in rows],
    }


@router.get("/usage/timeline")
async def usage_timeline(
    days: int = Query(7, ge=1, le=90),
    group_by: str = Query("hour", regex="^(hour|day)$"),
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/usage/timeline — Time-series usage data for charts."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    trunc = "hour" if group_by == "hour" else "day"

    result = await db.execute(text(f"""
        SELECT
            date_trunc(:trunc, timestamp) AS period,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE status = 'error') AS errors,
            ROUND(COALESCE(SUM(estimated_cost_usd), 0)::numeric, 6) AS cost_usd
        FROM usage_log
        WHERE timestamp >= :since
        GROUP BY period
        ORDER BY period ASC
    """), {"since": since, "trunc": trunc})

    rows = result.mappings().all()
    return {
        "period_days": days,
        "group_by": group_by,
        "data": [{"period": r["period"].isoformat(), "calls": r["total_calls"],
                   "errors": r["errors"], "cost_usd": float(r["cost_usd"])}
                 for r in rows],
    }


@router.get("/usage/errors")
async def usage_errors(
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """GET /api/admin/usage/errors — Recent error log for debugging."""
    since = datetime.now(timezone.utc) - timedelta(days=days)

    result = await db.execute(text("""
        SELECT
            timestamp, module, service, operation,
            error_message, request_summary, duration_ms,
            case_id, user_id
        FROM usage_log
        WHERE status = 'error' AND timestamp >= :since
        ORDER BY timestamp DESC
        LIMIT :limit
    """), {"since": since, "limit": limit})

    rows = result.mappings().all()
    return {"total_errors": len(rows), "errors": [dict(r) for r in rows]}

"""CLI commands — admin seeding."""

import asyncio
import sys

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .db.database import async_session_factory, init_db
from .models.user import User
from .auth.security import hash_password


async def seed_admin():
    """Create admin user from env vars if not already present."""
    if not settings.ADMIN_EMAIL or not settings.ADMIN_PASSWORD:
        print("ADMIN_EMAIL and ADMIN_PASSWORD must be set")
        return

    await init_db()

    async with async_session_factory() as db:
        result = await db.execute(select(User).where(User.email == settings.ADMIN_EMAIL))
        existing = result.scalar_one_or_none()
        if existing:
            print(f"Admin user already exists: {settings.ADMIN_EMAIL}")
            return

        admin = User(
            email=settings.ADMIN_EMAIL,
            hashed_password=hash_password(settings.ADMIN_PASSWORD),
            full_name=settings.ADMIN_NAME,
            role="admin",
            is_demo=False,
        )
        db.add(admin)
        await db.commit()
        print(f"Admin user created: {settings.ADMIN_EMAIL}")


async def auto_seed_admin():
    """Auto-seed admin during app startup (non-blocking, silent on missing env vars)."""
    if not settings.ADMIN_EMAIL or not settings.ADMIN_PASSWORD:
        return

    async with async_session_factory() as db:
        result = await db.execute(select(User).where(User.email == settings.ADMIN_EMAIL))
        if result.scalar_one_or_none():
            return

        admin = User(
            email=settings.ADMIN_EMAIL,
            hashed_password=hash_password(settings.ADMIN_PASSWORD),
            full_name=settings.ADMIN_NAME,
            role="admin",
            is_demo=False,
        )
        db.add(admin)
        await db.commit()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seed-admin":
        asyncio.run(seed_admin())
    else:
        print("Usage: python -m app.cli seed-admin")

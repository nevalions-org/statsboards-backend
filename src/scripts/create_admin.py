#!/usr/bin/env python3
"""Create admin user from environment variables.

This script creates an admin user in the database. It is designed to be
run as a Kubernetes Job during deployment.

Environment variables:
    ADMIN_USERNAME: Admin username
    ADMIN_EMAIL: Admin email
    ADMIN_PASSWORD: Admin password (required)

Exit codes:
    0: Success (user created or already exists)
    1: Error (missing password or database error)
"""

import asyncio
import os
import sys

from sqlalchemy import select

from src.auth.security import get_password_hash
from src.core.config import settings
from src.core.models import RoleDB, UserDB, UserRoleDB
from src.core.models.base import Database


async def create_admin() -> int:
    """Create admin user from environment variables."""
    username = os.getenv("ADMIN_USERNAME", "admin")
    email = os.getenv("ADMIN_EMAIL", "admin@example.com")
    password = os.getenv("ADMIN_PASSWORD")

    if not password:
        print("ERROR: ADMIN_PASSWORD environment variable not set")
        return 1

    print(f"Creating admin user: {username} ({email})")
    print(f"DB_HOST: {os.getenv('DB_HOST')}")
    print(f"DB_NAME: {os.getenv('DB_NAME')}")

    db = Database(settings.db.db_url)

    try:
        async with db.get_session_maker()() as session:
            result = await session.execute(select(RoleDB).where(RoleDB.name == "admin"))
            admin_role = result.scalar_one_or_none()

            if not admin_role:
                print("ERROR: Admin role not found. Run alembic migrations first.")
                return 1

            result = await session.execute(select(UserDB).where(UserDB.username == username))
            existing_user = result.scalar_one_or_none()

            if existing_user:
                existing_user.email = email
                existing_user.hashed_password = get_password_hash(password)
                await session.flush()

                result = await session.execute(
                    select(UserRoleDB).where(
                        UserRoleDB.user_id == existing_user.id,
                        UserRoleDB.role_id == admin_role.id,
                    )
                )
                if not result.scalar_one_or_none():
                    session.add(UserRoleDB(user_id=existing_user.id, role_id=admin_role.id))

                await session.commit()
                print(f"Updated existing user '{username}' with new credentials and admin role")
                return 0

            user = UserDB(
                username=username,
                email=email,
                hashed_password=get_password_hash(password),
                is_active=True,
            )
            session.add(user)
            await session.flush()

            session.add(UserRoleDB(user_id=user.id, role_id=admin_role.id))
            await session.commit()

            print(f"Admin user '{username}' created successfully")
            return 0

    except Exception as e:
        import traceback

        print(f"ERROR: Failed to create admin user: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(create_admin()))

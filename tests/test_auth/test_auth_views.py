"""Test auth views/endpoints."""

import pytest
import pytest_asyncio
from httpx import AsyncClient

from src.auth.security import create_access_token
from src.core.models import RoleDB, UserDB, UserRoleDB
from src.core.models.base import Database
from src.users.db_services import UserServiceDB
from src.users.schemas import UserSchemaCreate

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest_asyncio.fixture
async def test_user_with_role(test_db: Database):
    """Create a test user with a role."""
    from src.auth.security import get_password_hash
    from src.users.db_services import UserServiceDB

    UserServiceDB(test_db)

    async with test_db.get_session_maker()() as db_session:
        role = RoleDB(name="test_role", description="Test role")
        db_session.add(role)
        await db_session.flush()

        user_data = UserSchemaCreate(
            username="test_api_user",
            email="test_api@example.com",
            password="SecurePass123!",
        )
        user = UserDB(
            username=user_data.username,
            email=user_data.email,
            hashed_password=get_password_hash(user_data.password),
        )
        db_session.add(user)
        await db_session.flush()

        db_session.add(UserRoleDB(user_id=user.id, role_id=role.id))
        await db_session.flush()

        await db_session.refresh(user)
        await db_session.refresh(user, ["roles"])

        yield user

        await db_session.flush()


class TestAuthViews:
    """Test authentication API endpoints."""

    async def test_login_success(self, client: AsyncClient, test_db):
        """Test successful login returns token."""
        user_data = UserSchemaCreate(
            username="testuser_login",
            email="testuser_login@example.com",
            password="SecurePass123!",
        )

        service = UserServiceDB(test_db)
        await service.create(user_data)

        response = await client.post(
            "/api/auth/login",
            data={"username": "testuser_login", "password": "SecurePass123!"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    async def test_login_invalid_credentials(self, client: AsyncClient):
        """Test login fails with invalid credentials."""
        response = await client.post(
            "/api/auth/login",
            data={"username": "wronguser", "password": "wrongpass"},
        )

        assert response.status_code == 401
        data = response.json()
        assert "detail" in data

    async def test_me_authenticated(self, client: AsyncClient, test_user_with_role):
        """Test /me endpoint with valid token."""
        token = create_access_token(data={"sub": str(test_user_with_role.id)})

        response = await client.get(
            "/api/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == test_user_with_role.id
        assert data["username"] == "test_api_user"
        assert "created" in data
        assert "last_online" in data
        assert "is_online" in data
        assert isinstance(data["created"], str)

    async def test_me_no_token(self, client: AsyncClient):
        """Test /me endpoint without token returns 401."""
        response = await client.get("/api/auth/me")

        assert response.status_code == 401

    async def test_login_inactive_user(self, client: AsyncClient, test_db):
        """Test login fails for inactive user."""
        from src.users.db_services import UserServiceDB
        from src.users.schemas import UserSchemaUpdate

        user_data = UserSchemaCreate(
            username="inactive_user",
            email="inactive@example.com",
            password="SecurePass123!",
        )

        service = UserServiceDB(test_db)
        user = await service.create(user_data)

        await service.update(user.id, UserSchemaUpdate(is_active=False))

        response = await client.post(
            "/api/auth/login",
            data={"username": "inactive_user", "password": "SecurePass123!"},
        )

        assert response.status_code == 403

    async def test_heartbeat_success(self, client: AsyncClient, test_user_with_role, test_db):
        """Test heartbeat endpoint updates last_online and is_online."""

        token = create_access_token(data={"sub": str(test_user_with_role.id)})

        response = await client.post(
            "/api/auth/heartbeat",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 204

        user_service = UserServiceDB(test_db)
        user = await user_service.get_by_id_with_roles(test_user_with_role.id)

        assert user.last_online is not None
        assert user.is_online is True

    async def test_heartbeat_no_token(self, client: AsyncClient):
        """Test heartbeat endpoint without token returns 401."""
        response = await client.post("/api/auth/heartbeat")

        assert response.status_code == 401

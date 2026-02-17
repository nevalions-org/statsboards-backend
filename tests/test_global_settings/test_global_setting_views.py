import uuid

import pytest
import pytest_asyncio

from src.auth.security import create_access_token, get_password_hash
from src.core.models import RoleDB, UserDB
from src.global_settings.schemas import GlobalSettingSchemaUpdate
from src.users.schemas import UserSchemaCreate


@pytest_asyncio.fixture
async def admin_user(test_db):
    """Create a test admin user."""
    from src.users.db_services import UserServiceDB

    UserServiceDB(test_db)

    async with test_db.get_session_maker()() as db_session:
        role = RoleDB(name="admin", description="Admin role")
        db_session.add(role)
        await db_session.flush()
        await db_session.refresh(role)

        user_data = UserSchemaCreate(
            username="test_admin",
            email="admin@example.com",
            password="SecurePass123!",
        )
        user_obj = UserDB(
            username=user_data.username,
            email=user_data.email,
            hashed_password=get_password_hash(user_data.password),
        )
        user_obj.roles = [role]
        db_session.add(user_obj)
        await db_session.flush()
        await db_session.refresh(user_obj)

        token = create_access_token(data={"sub": str(user_obj.id)})

        yield {"user": user_obj, "token": token, "headers": {"Authorization": f"Bearer {token}"}}


@pytest_asyncio.fixture
async def admin_token(admin_user):
    """Get admin token for authenticated requests."""
    return admin_user["token"]


@pytest_asyncio.fixture
async def admin_headers(admin_user):
    """Get admin headers for authenticated requests."""
    return admin_user["headers"]


@pytest.mark.asyncio
class TestGlobalSettingViews:
    async def test_create_setting_endpoint(self, client, admin_headers):
        unique_suffix = uuid.uuid4().hex
        setting_data = {
            "key": f"test.create.{unique_suffix}.key",
            "value": "test_value",
            "value_type": "string",
            "category": "test",
            "description": "Test setting",
        }

        response = await client.post("/api/settings/", json=setting_data, headers=admin_headers)

        assert response.status_code == 200
        assert response.json()["id"] > 0
        assert response.json()["key"] == f"test.create.{unique_suffix}.key"

    async def test_create_setting_unauthorized(self, client):
        setting_data = {
            "key": f"test.unauthorized.{uuid.uuid4().hex}.key",
            "value": "test_value",
            "value_type": "string",
            "category": "test",
            "description": "Test setting",
        }

        response = await client.post("/api/settings/", json=setting_data)

        assert response.status_code == 401

    async def test_update_setting_unauthorized(self, client):
        update_data = GlobalSettingSchemaUpdate(value="updated_value")

        response = await client.put("/api/settings/1/", json=update_data.model_dump())

        assert response.status_code == 401

    @pytest.mark.slow
    async def test_get_setting_value_not_found(self, client):
        response = await client.get("/api/settings/value/nonexistent.key")

        assert response.status_code == 404

    async def test_delete_setting_unauthorized(self, client):
        response = await client.delete("/api/settings/id/1")

        assert response.status_code == 401

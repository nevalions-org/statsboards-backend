from unittest.mock import patch

import pytest

from src.helpers.photo_utils import photo_files_exist


class TestPhotoFilesExist:
    """Test suite for photo_files_exist function."""

    @pytest.fixture
    def temp_uploads_dir(self, tmp_path):
        """Create a temporary uploads directory for testing."""
        persons_dir = tmp_path / "persons" / "photos"
        persons_dir.mkdir(parents=True, exist_ok=True)
        return tmp_path

    def test_photo_files_exist_with_empty_url(self):
        """Test photo_files_exist returns False for empty URL."""
        result = photo_files_exist("")
        assert result is False

    def test_photo_files_exist_with_none_url(self):
        """Test photo_files_exist returns False for None URL."""
        result = photo_files_exist(None)
        assert result is False

    def test_photo_files_exist_with_valid_original_file(self, temp_uploads_dir):
        """Test photo_files_exist returns True when original file exists with valid size."""
        persons_dir = temp_uploads_dir / "persons" / "photos"

        image_file = persons_dir / "test_photo.jpg"
        image_file.write_bytes(b"x" * 2000)

        from src.core.config import settings

        with patch.object(type(settings), "uploads_path", property(lambda self: temp_uploads_dir)):
            result = photo_files_exist("/persons/photos/test_photo.jpg")
            assert result is True

    def test_photo_files_exist_with_valid_icon_file(self, temp_uploads_dir):
        """Test photo_files_exist returns True when icon file exists with valid size."""
        persons_dir = temp_uploads_dir / "persons" / "photos"

        icon_file = persons_dir / "test_photo_100px.jpg"
        icon_file.write_bytes(b"x" * 1500)

        from src.core.config import settings

        with patch.object(type(settings), "uploads_path", property(lambda self: temp_uploads_dir)):
            result = photo_files_exist("/persons/photos/test_photo.jpg")
            assert result is True

    def test_photo_files_exist_with_valid_web_file(self, temp_uploads_dir):
        """Test photo_files_exist returns True when web file exists with valid size."""
        persons_dir = temp_uploads_dir / "persons" / "photos"

        web_file = persons_dir / "test_photo_400px.jpg"
        web_file.write_bytes(b"x" * 3000)

        from src.core.config import settings

        with patch.object(type(settings), "uploads_path", property(lambda self: temp_uploads_dir)):
            result = photo_files_exist("/persons/photos/test_photo.jpg")
            assert result is True

    def test_photo_files_exist_with_file_too_small(self, temp_uploads_dir):
        """Test photo_files_exist returns False when file exists but size is below minimum."""
        persons_dir = temp_uploads_dir / "persons" / "photos"

        image_file = persons_dir / "test_photo.jpg"
        image_file.write_bytes(b"x" * 500)

        from src.core.config import settings

        with patch.object(type(settings), "uploads_path", property(lambda self: temp_uploads_dir)):
            result = photo_files_exist("/persons/photos/test_photo.jpg")
            assert result is False

    def test_photo_files_exist_no_files_exist(self, temp_uploads_dir):
        """Test photo_files_exist returns False when no files exist."""
        temp_uploads_dir / "persons" / "photos"

        from src.core.config import settings

        with patch.object(type(settings), "uploads_path", property(lambda self: temp_uploads_dir)):
            result = photo_files_exist("/persons/photos/test_photo.jpg")
            assert result is False

    def test_photo_files_exist_with_exception(self):
        """Test photo_files_exist returns False when exception occurs."""
        result = photo_files_exist("invalid:///path")
        assert result is False

    def test_photo_files_exist_prefer_larger_file(self, temp_uploads_dir):
        """Test photo_files_exist returns True when at least one file meets minimum size."""
        persons_dir = temp_uploads_dir / "persons" / "photos"

        small_file = persons_dir / "test_photo.jpg"
        small_file.write_bytes(b"x" * 500)

        large_file = persons_dir / "test_photo_100px.jpg"
        large_file.write_bytes(b"x" * 2000)

        from src.core.config import settings

        with patch.object(type(settings), "uploads_path", property(lambda self: temp_uploads_dir)):
            result = photo_files_exist("/persons/photos/test_photo.jpg")
            assert result is True

"""Tests for cli/fix_helpers.py module."""

import os
import shutil
from unittest.mock import MagicMock

import pytest
from click import BadParameter

from geoparquet_io.cli.fix_helpers import (
    create_backup_if_needed,
    handle_fix_error,
    validate_remote_file_modification,
    verify_fixes,
)


class TestValidateRemoteFileModification:
    """Tests for validate_remote_file_modification function."""

    def test_local_file_returns_false(self):
        """Test that local files return False."""
        result = validate_remote_file_modification("/local/file.parquet", None, False)
        assert result is False

    def test_remote_file_with_fix_output_returns_true(self):
        """Test remote file with fix_output specified returns True without error."""
        result = validate_remote_file_modification(
            "s3://bucket/file.parquet", "s3://bucket/output.parquet", False
        )
        assert result is True

    def test_remote_file_without_overwrite_raises_error(self):
        """Test remote file without overwrite flag raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_remote_file_modification("s3://bucket/file.parquet", None, False)
        assert "Cannot modify remote file" in str(exc_info.value)

    def test_remote_file_with_overwrite_returns_true(self):
        """Test remote file with overwrite flag returns True."""
        result = validate_remote_file_modification("s3://bucket/file.parquet", None, True)
        assert result is True


class TestCreateBackupIfNeeded:
    """Tests for create_backup_if_needed function."""

    def test_creates_backup_for_local_file(self, places_test_file, temp_output_dir):
        """Test that backup is created for local file."""
        # Copy test file to temp dir
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_test_file, test_file)

        result = create_backup_if_needed(
            parquet_file=test_file,
            output_path=test_file,
            no_backup=False,
            is_remote=False,
            verbose=False,
        )

        assert result == f"{test_file}.bak"
        assert os.path.exists(f"{test_file}.bak")

    def test_no_backup_with_no_backup_flag(self, places_test_file, temp_output_dir):
        """Test that no backup is created when no_backup is True."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_test_file, test_file)

        result = create_backup_if_needed(
            parquet_file=test_file,
            output_path=test_file,
            no_backup=True,
            is_remote=False,
            verbose=False,
        )

        assert result is None
        assert not os.path.exists(f"{test_file}.bak")

    def test_no_backup_for_different_output_path(self, places_test_file, temp_output_dir):
        """Test that no backup is created when output differs from input."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        output_file = os.path.join(temp_output_dir, "output.parquet")
        shutil.copy2(places_test_file, test_file)

        result = create_backup_if_needed(
            parquet_file=test_file,
            output_path=output_file,
            no_backup=False,
            is_remote=False,
            verbose=False,
        )

        assert result is None
        assert not os.path.exists(f"{test_file}.bak")

    def test_no_backup_for_remote_file(self):
        """Test that no backup is created for remote files."""
        result = create_backup_if_needed(
            parquet_file="s3://bucket/file.parquet",
            output_path="s3://bucket/file.parquet",
            no_backup=False,
            is_remote=True,
            verbose=False,
        )

        assert result is None

    def test_creates_backup_with_verbose(self, places_test_file, temp_output_dir):
        """Test backup creation with verbose flag."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_test_file, test_file)

        result = create_backup_if_needed(
            parquet_file=test_file,
            output_path=test_file,
            no_backup=False,
            is_remote=False,
            verbose=True,
        )

        assert result == f"{test_file}.bak"
        assert os.path.exists(f"{test_file}.bak")


class TestVerifyFixes:
    """Tests for verify_fixes function."""

    def test_all_checks_pass(self):
        """Test verification when all checks pass."""
        mock_structure = MagicMock(
            return_value={
                "row_groups": {"passed": True},
                "bbox": {"passed": True},
                "compression": {"passed": True},
            }
        )
        mock_spatial = MagicMock(return_value={"passed": True})

        result = verify_fixes(
            output_path="test.parquet",
            check_structure_impl=mock_structure,
            check_spatial_impl=mock_spatial,
            random_sample_size=50,
            limit_rows=500,
        )

        assert result is True

    def test_some_checks_fail(self):
        """Test verification when some checks fail."""
        mock_structure = MagicMock(
            return_value={
                "row_groups": {"passed": True},
                "bbox": {"passed": False, "issues": ["Missing bbox column"]},
                "compression": {"passed": True},
            }
        )
        mock_spatial = MagicMock(return_value={"passed": True})

        result = verify_fixes(
            output_path="test.parquet",
            check_structure_impl=mock_structure,
            check_spatial_impl=mock_spatial,
            random_sample_size=50,
            limit_rows=500,
        )

        assert result is False

    def test_spatial_check_fails(self):
        """Test verification when spatial check fails."""
        mock_structure = MagicMock(
            return_value={
                "row_groups": {"passed": True},
                "bbox": {"passed": True},
                "compression": {"passed": True},
            }
        )
        mock_spatial = MagicMock(
            return_value={
                "passed": False,
                "issues": ["Spatial ordering suboptimal"],
            }
        )

        result = verify_fixes(
            output_path="test.parquet",
            check_structure_impl=mock_structure,
            check_spatial_impl=mock_spatial,
            random_sample_size=50,
            limit_rows=500,
        )

        assert result is False

    def test_check_without_issues_list(self):
        """Test verification when failing check has no issues list."""
        mock_structure = MagicMock(
            return_value={
                "row_groups": {"passed": False},  # No issues key
                "bbox": {"passed": True},
                "compression": {"passed": True},
            }
        )
        mock_spatial = MagicMock(return_value={"passed": True})

        result = verify_fixes(
            output_path="test.parquet",
            check_structure_impl=mock_structure,
            check_spatial_impl=mock_spatial,
            random_sample_size=50,
            limit_rows=500,
        )

        assert result is False


class TestHandleFixError:
    """Tests for handle_fix_error function."""

    def test_restores_backup_on_error(self, places_test_file, temp_output_dir):
        """Test that backup is restored when an error occurs."""
        # Set up test file and backup
        test_file = os.path.join(temp_output_dir, "test.parquet")
        backup_path = f"{test_file}.bak"
        shutil.copy2(places_test_file, test_file)
        shutil.copy2(places_test_file, backup_path)

        # Modify the test file to simulate a failed fix
        with open(test_file, "wb") as f:
            f.write(b"corrupted")

        # Call handle_fix_error
        handle_fix_error(
            e=Exception("Fix failed"),
            no_backup=False,
            output_path=test_file,
            parquet_file=test_file,
            backup_path=backup_path,
        )

        # Verify backup was restored and removed
        assert os.path.exists(test_file)
        assert not os.path.exists(backup_path)
        # Verify file was restored (not corrupted)
        with open(test_file, "rb") as f:
            content = f.read()
            assert content != b"corrupted"

    def test_no_restore_when_no_backup_true(self, temp_output_dir):
        """Test that no restore happens when no_backup is True."""
        test_file = os.path.join(temp_output_dir, "test.parquet")

        # This should not raise an error even without backup
        handle_fix_error(
            e=Exception("Fix failed"),
            no_backup=True,
            output_path=test_file,
            parquet_file=test_file,
            backup_path=None,
        )

    def test_no_restore_when_different_output_path(self, temp_output_dir):
        """Test that no restore happens when output differs from input."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        output_file = os.path.join(temp_output_dir, "output.parquet")

        handle_fix_error(
            e=Exception("Fix failed"),
            no_backup=False,
            output_path=output_file,
            parquet_file=test_file,
            backup_path=None,
        )

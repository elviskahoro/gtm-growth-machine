from __future__ import annotations

from enum import Enum
from pathlib import Path


class DestinationType(str, Enum):
    LOCAL = "local"
    GCP = "gcp"

    @staticmethod
    def get_bucket_url_for_local(
        pipeline_name: str,
    ) -> str:
        cwd: str = str(Path.cwd())
        return f"{cwd}/out/{pipeline_name}"


# trunk-ignore-begin(ruff/PLR2004,ruff/S101,ruff/PLC0415)
def test_destination_type_enum_values() -> None:
    """Test DestinationType enum has correct values."""
    assert DestinationType.LOCAL == "local"
    assert DestinationType.GCP == "gcp"

    # Test that enum values are strings
    assert isinstance(DestinationType.LOCAL, str)
    assert isinstance(DestinationType.GCP, str)


def test_destination_type_enum_inheritance() -> None:
    """Test DestinationType inherits from both str and Enum."""
    from enum import Enum

    # Test inheritance
    assert issubclass(DestinationType, str)
    assert issubclass(DestinationType, Enum)

    # Test instance types
    local_type: DestinationType = DestinationType.LOCAL
    gcp_type: DestinationType = DestinationType.GCP

    assert isinstance(local_type, str)
    assert isinstance(local_type, Enum)
    assert isinstance(gcp_type, str)
    assert isinstance(gcp_type, Enum)


def test_destination_type_enum_comparison() -> None:
    """Test DestinationType enum comparison operations."""
    # Test equality with string values
    assert DestinationType.LOCAL == "local"
    assert DestinationType.GCP == "gcp"

    # Test inequality
    assert DestinationType.LOCAL != "gcp"
    assert DestinationType.GCP != "local"

    # Test enum member equality
    assert DestinationType.LOCAL == DestinationType.LOCAL
    assert DestinationType.GCP == DestinationType.GCP
    assert DestinationType.LOCAL != DestinationType.GCP


def test_destination_type_enum_iteration() -> None:
    """Test DestinationType enum iteration."""
    enum_values: list[DestinationType] = list(DestinationType)

    assert len(enum_values) == 2
    assert DestinationType.LOCAL in enum_values
    assert DestinationType.GCP in enum_values

    # Test that we can iterate and get string values
    string_values: list[str] = [str(enum_val) for enum_val in DestinationType]
    # Note: str() on enum returns the enum name, not the value for this enum type
    assert "DestinationType.LOCAL" in string_values
    assert "DestinationType.GCP" in string_values


def test_get_bucket_url_for_local_basic() -> None:
    """Test get_bucket_url_for_local with basic pipeline name."""
    from unittest.mock import patch

    pipeline_name: str = "test_pipeline"
    mock_cwd: str = "/home/user/project"

    with patch("pathlib.Path.cwd", return_value=Path(mock_cwd)):
        result: str = DestinationType.get_bucket_url_for_local(pipeline_name)
        expected: str = f"{mock_cwd}/out/{pipeline_name}"

        assert result == expected
        assert result == "/home/user/project/out/test_pipeline"


def test_get_bucket_url_for_local_different_pipeline_names() -> None:
    """Test get_bucket_url_for_local with various pipeline names."""
    from unittest.mock import patch

    mock_cwd: str = "/home/test"

    test_cases: list[tuple[str, str]] = [
        ("simple", "/home/test/out/simple"),
        ("data-pipeline", "/home/test/out/data-pipeline"),
        ("pipeline_with_underscores", "/home/test/out/pipeline_with_underscores"),
        ("123-numeric-pipeline", "/home/test/out/123-numeric-pipeline"),
        ("CapitalCase", "/home/test/out/CapitalCase"),
    ]

    with patch("pathlib.Path.cwd", return_value=Path(mock_cwd)):
        for pipeline_name, expected_url in test_cases:
            result: str = DestinationType.get_bucket_url_for_local(pipeline_name)
            assert result == expected_url


def test_get_bucket_url_for_local_different_working_directories() -> None:
    """Test get_bucket_url_for_local with different working directories."""
    from unittest.mock import patch

    pipeline_name: str = "test"

    test_cases: list[tuple[str, str]] = [
        ("/", "//out/test"),
        ("/home/user", "/home/user/out/test"),
        ("/var/lib/app", "/var/lib/app/out/test"),
        ("/Users/developer/projects/myapp", "/Users/developer/projects/myapp/out/test"),
        (
            "C:\\Users\\Windows\\Project",
            "C:\\Users\\Windows\\Project/out/test",
        ),  # Windows path
    ]

    for cwd_path, expected_url in test_cases:
        with patch("pathlib.Path.cwd", return_value=Path(cwd_path)):
            result: str = DestinationType.get_bucket_url_for_local(pipeline_name)
            assert result == expected_url


def test_get_bucket_url_for_local_empty_pipeline_name() -> None:
    """Test get_bucket_url_for_local with empty pipeline name."""
    from unittest.mock import patch

    mock_cwd: str = "/home/test"
    empty_pipeline_name: str = ""

    with patch("pathlib.Path.cwd", return_value=Path(mock_cwd)):
        result: str = DestinationType.get_bucket_url_for_local(empty_pipeline_name)
        expected: str = f"{mock_cwd}/out/"

        assert result == expected
        assert result == "/home/test/out/"


def test_get_bucket_url_for_local_special_characters() -> None:
    """Test get_bucket_url_for_local with special characters in pipeline name."""
    from unittest.mock import patch

    mock_cwd: str = "/test"

    test_cases: list[tuple[str, str]] = [
        ("pipeline with spaces", "/test/out/pipeline with spaces"),
        ("pipeline@special#chars", "/test/out/pipeline@special#chars"),
        ("pipeline/with/slashes", "/test/out/pipeline/with/slashes"),
        ("pipeline\\with\\backslashes", "/test/out/pipeline\\with\\backslashes"),
        ("unicode-πüñeliñe", "/test/out/unicode-πüñeliñe"),
    ]

    with patch("pathlib.Path.cwd", return_value=Path(mock_cwd)):
        for pipeline_name, expected_url in test_cases:
            result: str = DestinationType.get_bucket_url_for_local(pipeline_name)
            assert result == expected_url


def test_get_bucket_url_for_local_static_method() -> None:
    """Test that get_bucket_url_for_local is a static method."""
    from unittest.mock import patch

    mock_cwd: str = "/static/test"
    pipeline_name: str = "static_test"

    with patch("pathlib.Path.cwd", return_value=Path(mock_cwd)):
        # Should be callable without instance
        result1: str = DestinationType.get_bucket_url_for_local(pipeline_name)

        # Should also be callable from an enum instance
        local_instance: DestinationType = DestinationType.LOCAL
        result2: str = local_instance.get_bucket_url_for_local(pipeline_name)

        # Both should return the same result
        assert result1 == result2
        assert result1 == "/static/test/out/static_test"


def test_get_bucket_url_for_local_path_construction() -> None:
    """Test that get_bucket_url_for_local constructs paths correctly."""
    from unittest.mock import patch

    # Test that Path.cwd() is called correctly
    mock_path: Path = Path("/mock/directory")
    pipeline_name: str = "path_test"

    with patch("pathlib.Path.cwd", return_value=mock_path) as mock_cwd:
        result: str = DestinationType.get_bucket_url_for_local(pipeline_name)

        # Verify Path.cwd() was called
        mock_cwd.assert_called_once()

        # Verify result format
        assert result.startswith("/mock/directory/out/")
        assert result.endswith("/path_test")
        assert result == "/mock/directory/out/path_test"


def test_destination_type_enum_string_representation() -> None:
    """Test string representation and conversion of DestinationType enum."""
    # Test str() conversion - for this enum, str() returns the enum name
    assert str(DestinationType.LOCAL) == "DestinationType.LOCAL"
    assert str(DestinationType.GCP) == "DestinationType.GCP"

    # Test value access - the actual string values
    assert DestinationType.LOCAL.value == "local"
    assert DestinationType.GCP.value == "gcp"

    # Test repr() representation
    local_repr: str = repr(DestinationType.LOCAL)
    gcp_repr: str = repr(DestinationType.GCP)

    assert "DestinationType.LOCAL" in local_repr
    assert "DestinationType.GCP" in gcp_repr


def test_destination_type_enum_membership() -> None:
    """Test membership operations with DestinationType enum."""
    # Test 'in' operator with enum values
    all_types: list[DestinationType] = [DestinationType.LOCAL, DestinationType.GCP]

    assert DestinationType.LOCAL in all_types
    assert DestinationType.GCP in all_types

    # Test with string values
    string_values: list[str] = ["local", "gcp"]
    assert DestinationType.LOCAL in string_values  # Should work due to str inheritance
    assert DestinationType.GCP in string_values


def test_destination_type_enum_creation_from_string() -> None:
    """Test creating DestinationType enum from string values."""
    # Test creating enum from valid string values
    local_from_string: DestinationType = DestinationType("local")
    gcp_from_string: DestinationType = DestinationType("gcp")

    assert local_from_string == DestinationType.LOCAL
    assert gcp_from_string == DestinationType.GCP

    # Test invalid string raises ValueError
    import pytest

    with pytest.raises(ValueError, match="'invalid' is not a valid DestinationType"):
        DestinationType("invalid")


def test_destination_type_enum_hash() -> None:
    """Test that DestinationType enum values are hashable."""
    # Test that enum values can be used as dictionary keys
    enum_dict: dict[DestinationType, str] = {
        DestinationType.LOCAL: "local_value",
        DestinationType.GCP: "gcp_value",
    }

    assert enum_dict[DestinationType.LOCAL] == "local_value"
    assert enum_dict[DestinationType.GCP] == "gcp_value"

    # Test that enum values can be added to sets
    enum_set: set[DestinationType] = {DestinationType.LOCAL, DestinationType.GCP}

    assert len(enum_set) == 2
    assert DestinationType.LOCAL in enum_set
    assert DestinationType.GCP in enum_set


def test_get_bucket_url_for_local_return_type() -> None:
    """Test that get_bucket_url_for_local returns correct type."""
    from unittest.mock import patch

    pipeline_name: str = "type_test"
    mock_cwd: str = "/type/test"

    with patch("pathlib.Path.cwd", return_value=Path(mock_cwd)):
        result: str = DestinationType.get_bucket_url_for_local(pipeline_name)

        assert isinstance(result, str)
        assert len(result) > 0
        assert "/" in result  # Should contain path separators


# trunk-ignore-end(ruff/PLR2004,ruff/S101,ruff/PLC0415)

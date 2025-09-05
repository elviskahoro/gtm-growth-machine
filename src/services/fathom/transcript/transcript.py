from pydantic import BaseModel


class Transcript(BaseModel):
    plaintext: str


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_transcript_creation_with_valid_plaintext() -> None:
    """Test creating a Transcript with valid plaintext."""
    plaintext: str = "This is a sample transcript."
    transcript: Transcript = Transcript(plaintext=plaintext)

    assert transcript.plaintext == plaintext
    assert isinstance(transcript, Transcript)


def test_transcript_creation_with_empty_string() -> None:
    """Test creating a Transcript with empty plaintext."""
    plaintext: str = ""
    transcript: Transcript = Transcript(plaintext=plaintext)

    assert transcript.plaintext == ""


def test_transcript_creation_with_multiline_text() -> None:
    """Test creating a Transcript with multiline plaintext."""
    plaintext: str = "Line 1\nLine 2\nLine 3"
    transcript: Transcript = Transcript(plaintext=plaintext)

    assert transcript.plaintext == plaintext
    assert "\n" in transcript.plaintext


def test_transcript_creation_with_unicode_characters() -> None:
    """Test creating a Transcript with unicode characters."""
    plaintext: str = "Hello 世界! 🌍 émojis and spëcial chars"
    transcript: Transcript = Transcript(plaintext=plaintext)

    assert transcript.plaintext == plaintext


def test_transcript_creation_with_very_long_text() -> None:
    """Test creating a Transcript with very long plaintext."""
    plaintext: str = "word " * 10000  # 50,000 characters
    transcript: Transcript = Transcript(plaintext=plaintext)

    assert transcript.plaintext == plaintext
    assert len(transcript.plaintext) == 50000


def test_transcript_missing_plaintext_field() -> None:
    """Test that creating a Transcript without plaintext raises ValidationError."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        Transcript()  # type: ignore[call-arg]

    error: ValidationError = exc_info.value
    assert "plaintext" in str(error)
    assert "Field required" in str(error)


def test_transcript_none_plaintext_raises_error() -> None:
    """Test that creating a Transcript with None plaintext raises ValidationError."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        Transcript(plaintext=None)  # type: ignore[arg-type]

    error: ValidationError = exc_info.value
    assert "plaintext" in str(error)


def test_transcript_invalid_type_raises_error() -> None:
    """Test that creating a Transcript with non-string plaintext raises ValidationError."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        Transcript(plaintext=123)  # type: ignore[arg-type]

    error: ValidationError = exc_info.value
    assert "plaintext" in str(error)


def test_transcript_equality() -> None:
    """Test that two Transcript instances with same plaintext are equal."""
    plaintext: str = "Same transcript content"
    transcript1: Transcript = Transcript(plaintext=plaintext)
    transcript2: Transcript = Transcript(plaintext=plaintext)

    assert transcript1 == transcript2


def test_transcript_inequality() -> None:
    """Test that two Transcript instances with different plaintext are not equal."""
    transcript1: Transcript = Transcript(plaintext="First transcript")
    transcript2: Transcript = Transcript(plaintext="Second transcript")

    assert transcript1 != transcript2


def test_transcript_dict_conversion() -> None:
    """Test converting Transcript to dictionary."""
    plaintext: str = "Test transcript for dict conversion"
    transcript: Transcript = Transcript(plaintext=plaintext)

    transcript_dict: dict[str, str] = transcript.model_dump()
    expected_dict: dict[str, str] = {"plaintext": plaintext}

    assert transcript_dict == expected_dict


def test_transcript_json_serialization() -> None:
    """Test JSON serialization of Transcript."""
    plaintext: str = "Test transcript for JSON"
    transcript: Transcript = Transcript(plaintext=plaintext)

    json_str: str = transcript.model_dump_json()
    assert plaintext in json_str
    assert '"plaintext"' in json_str


def test_transcript_from_dict() -> None:
    """Test creating Transcript from dictionary."""
    data: dict[str, str] = {"plaintext": "Transcript from dict"}
    transcript: Transcript = Transcript(**data)

    assert transcript.plaintext == "Transcript from dict"


# trunk-ignore-end(ruff/PLR2004,ruff/S101)

import re

FILE_SYSTEM_TRANSLATION: dict[str, str] = {
    " ": "·",
    "/": "_",
    "\\": "_",
    "(": "",
    ")": "",
    "[": "",
    "]": "",
    "{": "",
    "}": "",
    "<": "",
    ">": "",
    "|": "",
    ":": "",
    ",": "",
    ".": "",
    "!": "",
    "?": "",
    "'": "",
}


def sanitize_string(
    string: str,
) -> str:
    translation_map: dict[str, str] = FILE_SYSTEM_TRANSLATION.copy()
    return re.sub(
        "|".join(map(re.escape, translation_map.keys())),
        lambda m: translation_map[m.group()],
        string,
    )


# trunk-ignore-begin(ruff/S101,ruff/PLC0415)
def test_sanitize_string_basic_characters() -> None:
    """Test sanitizing basic problematic filesystem characters."""
    # Test individual character replacements
    assert sanitize_string(" ") == "·"
    assert sanitize_string("/") == "_"
    assert sanitize_string("\\") == "_"
    assert sanitize_string("(") == ""
    assert sanitize_string(")") == ""
    assert sanitize_string("[") == ""
    assert sanitize_string("]") == ""
    assert sanitize_string("{") == ""
    assert sanitize_string("}") == ""
    assert sanitize_string("<") == ""
    assert sanitize_string(">") == ""
    assert sanitize_string("|") == ""
    assert sanitize_string(":") == ""
    assert sanitize_string(",") == ""
    assert sanitize_string(".") == ""
    assert sanitize_string("!") == ""
    assert sanitize_string("?") == ""
    assert sanitize_string("'") == ""


def test_sanitize_string_empty_input() -> None:
    """Test sanitizing empty string."""
    assert sanitize_string("") == ""


def test_sanitize_string_no_special_characters() -> None:
    """Test sanitizing strings with no problematic characters."""
    test_strings: list[str] = [
        "hello",
        "test123",
        "ValidFileName",
        "no-special-chars",
        "underscores_are_fine",
        "hyphen-test",
        "numbers123456",
        "MixedCase",
    ]

    for test_string in test_strings:
        assert sanitize_string(test_string) == test_string


def test_sanitize_string_combined_characters() -> None:
    """Test sanitizing strings with multiple problematic characters."""
    test_cases: list[tuple[str, str]] = [
        ("hello world", "hello·world"),
        ("file/path\\name", "file_path_name"),
        ("test(1)", "test1"),
        ("data[1]", "data1"),
        ("config{dev}", "configdev"),
        ("input<output>", "inputoutput"),
        ("pipe|test", "pipetest"),
        ("time:stamp", "timestamp"),
        ("item1,item2", "item1item2"),
        ("file.txt", "filetxt"),
        ("urgent!", "urgent"),
        ("question?", "question"),
        ("user's file", "users·file"),
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_complex_filenames() -> None:
    """Test sanitizing complex filename-like strings."""
    test_cases: list[tuple[str, str]] = [
        ("My Document (Final).txt", "My·Document·Finaltxt"),
        ("Project [v1.0].zip", "Project·v10zip"),
        ("Data/Analysis\\Results.csv", "Data_Analysis_Resultscsv"),
        ("User's Guide: Version 2.0!", "Users·Guide·Version·20"),
        ("Query<Results>{2023}.json", "QueryResults2023json"),
        ("File|Name?With!Special,Chars.", "FileNameWithSpecialChars"),
        ("Windows\\Path/Mixed\\Separators", "Windows_Path_Mixed_Separators"),
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_unicode_characters() -> None:
    """Test sanitizing strings with Unicode characters."""
    test_cases: list[tuple[str, str]] = [
        ("café", "café"),  # Unicode characters should be preserved
        ("naïve", "naïve"),
        ("Müller", "Müller"),
        ("José María", "José·María"),  # Space should still be replaced
        ("file.txt", "filetxt"),  # File with extension
        ("测试文档", "测试文档"),  # Chinese characters
        ("🎵music🎵", "🎵music🎵"),  # Emoji should be preserved
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_multiple_spaces() -> None:
    """Test sanitizing strings with multiple consecutive spaces."""
    test_cases: list[tuple[str, str]] = [
        ("hello  world", "hello··world"),
        ("test   file", "test···file"),
        ("   leading spaces", "···leading·spaces"),
        ("trailing spaces   ", "trailing·spaces···"),
        ("  both  sides  ", "··both··sides··"),
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_all_special_characters() -> None:
    """Test sanitizing a string containing all problematic characters."""
    all_special: str = " /\\()[]{}><|:,.!?'"
    expected: str = (
        "·__"  # Space becomes ·, slashes become _, everything else is removed
    )
    assert sanitize_string(all_special) == expected


def test_sanitize_string_repeated_characters() -> None:
    """Test sanitizing strings with repeated problematic characters."""
    test_cases: list[tuple[str, str]] = [
        ("////", "____"),
        ("....", ""),
        ("!!!!", ""),
        ("????", ""),
        ("(((())))", ""),
        ("[[[]]]", ""),
        ("{{{}}", ""),
        ("<<<>>>", ""),
        ("|||", ""),
        (":::", ""),
        (",,,", ""),
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_long_strings() -> None:
    """Test sanitizing very long strings."""
    # Test with a long string containing various characters
    long_input: str = "a" * 1000 + "/" + "b" * 1000 + "." + "c" * 1000
    expected: str = "a" * 1000 + "_" + "b" * 1000 + "c" * 1000
    assert sanitize_string(long_input) == expected

    # Test with long string of only problematic characters
    long_special: str = "/.!?" * 250  # 1000 characters total
    expected_special: str = "_" * 250  # Only slashes become underscores, others removed
    assert sanitize_string(long_special) == expected_special


def test_sanitize_string_real_world_examples() -> None:
    """Test sanitizing real-world filename examples."""
    test_cases: list[tuple[str, str]] = [
        ("Meeting Notes (2023-12-15).docx", "Meeting·Notes·2023-12-15docx"),
        ("Budget_Q4[Final Version].xlsx", "Budget_Q4Final·Versionxlsx"),
        ("Photo: Summer Vacation! (Beach).jpg", "Photo·Summer·Vacation·Beachjpg"),
        ("Code Review - Module A/B\\C.pdf", "Code·Review·-·Module·A_B_Cpdf"),
        ("User Manual v2.0 - What's New?.txt", "User·Manual·v20·-·Whats·Newtxt"),
        ("Data{2023}|Analysis<Results>.csv", "Data2023AnalysisResultscsv"),
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_edge_case_characters() -> None:
    """Test sanitizing with edge case character combinations."""
    test_cases: list[tuple[str, str]] = [
        ("file./name", "file_name"),  # Dot followed by slash
        ("path\\:file", "path_file"),  # Backslash followed by colon
        ("name()[]", "name"),  # Adjacent brackets and parentheses
        ("test|<>file", "testfile"),  # Adjacent pipe and angle brackets
        ("data!?.", "data"),  # Adjacent punctuation
        ("file'name", "filename"),  # Apostrophe in middle
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_file_system_translation_constant() -> None:
    """Test that the FILE_SYSTEM_TRANSLATION constant contains expected mappings."""
    expected_keys: set[str] = {
        " ",
        "/",
        "\\",
        "(",
        ")",
        "[",
        "]",
        "{",
        "}",
        "<",
        ">",
        "|",
        ":",
        ",",
        ".",
        "!",
        "?",
        "'",
    }

    assert set(FILE_SYSTEM_TRANSLATION.keys()) == expected_keys

    # Test specific mappings
    assert FILE_SYSTEM_TRANSLATION[" "] == "·"
    assert FILE_SYSTEM_TRANSLATION["/"] == "_"
    assert FILE_SYSTEM_TRANSLATION["\\"] == "_"

    # Test that removed characters map to empty string
    removed_chars: list[str] = [
        "(",
        ")",
        "[",
        "]",
        "{",
        "}",
        "<",
        ">",
        "|",
        ":",
        ",",
        ".",
        "!",
        "?",
        "'",
    ]
    for char in removed_chars:
        assert FILE_SYSTEM_TRANSLATION[char] == ""


def test_file_system_translation_immutability() -> None:
    """Test that the original FILE_SYSTEM_TRANSLATION is not modified by sanitize_string."""
    import copy

    original_translation: dict[str, str] = copy.deepcopy(FILE_SYSTEM_TRANSLATION)

    # Run sanitize_string multiple times with different inputs
    test_strings: list[str] = [
        "test/file\\name",
        "complex()[]{}string",
        "special|<>:,.!?'chars",
        "multiple   spaces",
    ]

    for test_string in test_strings:
        sanitize_string(test_string)

    # Verify the original dictionary is unchanged
    assert original_translation == FILE_SYSTEM_TRANSLATION


def test_sanitize_string_regex_special_characters() -> None:
    """Test that regex special characters in the input don't break the function."""
    # Test strings that contain regex metacharacters
    test_cases: list[tuple[str, str]] = [
        ("test+file", "test+file"),  # + is not in translation map
        ("file*name", "file*name"),  # * is not in translation map
        ("test^file", "test^file"),  # ^ is not in translation map
        ("file$name", "file$name"),  # $ is not in translation map
        ("test-file", "test-file"),  # - is not in translation map
        ("file[a-z]", "filea-z"),  # [ and ] are in translation map
        ("test(group)", "testgroup"),  # ( and ) are in translation map
    ]

    for input_str, expected in test_cases:
        assert sanitize_string(input_str) == expected


def test_sanitize_string_performance() -> None:
    """Test performance with various input sizes."""
    import time

    # Test with different string sizes
    test_sizes: list[int] = [10, 100, 1000, 10000]

    for size in test_sizes:
        test_string: str = "a/b\\c.d" * (size // 7)  # Mix of normal and special chars

        start_time: float = time.time()
        result: str = sanitize_string(test_string)
        end_time: float = time.time()

        # Verify the result is correct
        expected_pattern: str = "a_b_cd" * (size // 7)
        assert result == expected_pattern

        # Verify it completes in reasonable time (should be very fast)
        duration: float = end_time - start_time
        assert duration < 1.0  # Should complete in less than 1 second


# trunk-ignore-end(ruff/S101,ruff/PLC0415)

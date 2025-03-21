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

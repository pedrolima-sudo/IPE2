import re
from unidecode import unidecode


def normalize_name(name: str) -> str:
    if name is None:
        return ""
    s = unidecode(str(name)).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()

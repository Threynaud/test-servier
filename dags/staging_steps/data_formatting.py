"""
Utilities for data formatting during the staging steps.
"""

import codecs
import re

import unidecode
from dateutil.parser import parse

# Normalized and arbitrary date format to use across all staging files.
DATE_FORMAT = "%d/%m/%Y"


def preprocess_title(title: str) -> str:
    """Remove puntuation and other special characters (like the TM character) from a string then lower the text.

    Args:
        title: Title of the publication to preprocess.

    Returns:
        Preprocessed title.

    """

    preprocessed_title = remove_special_char(title)
    preprocessed_title = preprocessed_title.lower()
    return preprocessed_title


def normalize_date(date_str: str) -> str:
    """Parse a date string and format it to the format assigned to DATE_FORMAT above.

    Args:
        date_str: Date string.

    Returns:
        Formatted date string.

    """

    dt = parse(date_str)
    normalized_dt = dt.strftime(DATE_FORMAT)
    return normalized_dt


def remove_bytes(my_str: str) -> str:
    """Remove byte like characters (ie: '\xc03') from string.

    Args:
        my_str: Text to be cleaned

    Returns:
        Cleaned text.

    """
    my_str = re.sub(r"(\s*(\\x)\w+\s*)+", " ", my_str)
    my_str = my_str.strip()  # Remove potential space at the end of the string introduced by the operation above.
    return my_str


def remove_special_char(my_str: str) -> str:
    """Remove punctuation and other special characters from a string.

    Args:
        my_str: Text to be cleaned.

    Returns:
        Cleaned text.
    """

    str_f = my_str
    decoded_str = unidecode.unidecode(my_str)
    decoded_str = codecs.decode(decoded_str, "unicode_escape")  # type: ignore
    str_f = re.sub(r"[^A-Za-z0-9 ]+", "", decoded_str)
    return str_f

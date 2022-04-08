import codecs
import re

import unidecode
from dateutil.parser import parse

DATE_FORMAT = "%d/%m/%Y"


def preprocess_title(title):
    preprocessed_title = remove_special_char(title)
    preprocessed_title = preprocessed_title.lower()
    return preprocessed_title


def normalize_date(date_str):
    dt = parse(date_str)
    normalized_dt = dt.strftime(DATE_FORMAT)
    return normalized_dt


def remove_bytes(my_str):
    my_str = re.sub(r"(\s*(\\x)\w+\s*)+", " ", my_str)
    my_str = my_str.strip()  # Remove potential space at the end of the string introduced by the operation above.
    return my_str


def remove_special_char(my_str):
    """"""
    str_f = my_str
    my_str = unidecode.unidecode(my_str)
    my_str = codecs.decode(my_str, "unicode_escape")
    str_f = re.sub(r"[^A-Za-z0-9 ]+", "", my_str)
    return str_f

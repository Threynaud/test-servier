import re

from dateutil.parser import parse

DATE_FORMAT = "%d/%m/%Y"


def normalize_date(date_str):
    dt = parse(date_str)
    normalized_dt = dt.strftime(DATE_FORMAT)
    return normalized_dt


def remove_bytes(my_str):
    my_str = re.sub(r"(\s*(\\x)\w+\s*)+", " ", my_str)
    my_str = my_str.strip()  # Remove potential space at the end of the string introduced by the operation above.
    return my_str

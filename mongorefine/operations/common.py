"""Common string manipulation functions used with wrangling functions"""

from datetime import datetime, date

def do_copy(s):
    return s

def do_upper(s):
    return str(s).upper() if s else s

def do_lower(s):
    return str(s).lower() if s else s

def do_title(s):
    return str(s).title() if s else s

def  do_trim(s):
    return str(s).strip() if s else s

def do_float(s):
    return float(s) if s else s

def do_int(s):
    return int(s) if s else s

def do_string(s):
    return str(s) if s else s

def do_simple_datetime(s):
    return datetime.fromisoformat(s)

def do_simple_date(s):
    return date.fromisoformat(s)

import sys

# Useful for very coarse version differentiation.
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3
PY36 = sys.version_info[0:2] >= (3, 6)

# long type vary with python versions
if PY3:
    LONG = int
else:
    LONG = long


# convert string to bytes
def str_to_bytes(str_v):
    if PY3:
        if isinstance(str_v, str):
            return str_v.encode()
        elif isinstance(str_v, bytes):
            return str_v
        else:
            raise TypeError("str_to_bytes: str or bytes expected")
    else:
        return bytes(str_v)

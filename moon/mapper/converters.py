def to_integer(value):
    x = None
    try:
        x = int(value)
    finally:
        return x

def to_float(value):
    x = None
    try:
        x = float(value)
    finally:
        return x


def to_varchar(value):
    x = None
    try:
        x = str(value)
    finally:
        return x


def to_bool(value):
    x = None
    try:
        accepts = ['True', 'TRUE', '1', 1, 'False', 'FALSE', '0', 0]
        x = 'NULL' if value not in accepts else value
    finally:
        return x

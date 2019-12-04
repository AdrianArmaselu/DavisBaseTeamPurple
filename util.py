IS_DEBUG_LOGGING_ENABLED = True


def bytes_to_int(number_bytes):
    return int.from_bytes(number_bytes, 'big')


def int_to_bytes(number, size):
    return int.to_bytes(number, size, 'big')


def log_debug(*values):
    if IS_DEBUG_LOGGING_ENABLED:
        print("DEBUG TableFile -", *values)

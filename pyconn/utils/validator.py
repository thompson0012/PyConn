import six


def validate_keys(data: dict, require=None):
    keys_needed = set(require if require is not None else [])

    missing = keys_needed.difference(six.iterkeys(data))

    if missing:
        raise ValueError(
            "data was not in the expected format, missing "
            "fields {}.".format(", ".join(missing))
        )

    return True

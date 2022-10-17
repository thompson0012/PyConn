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


def validate_opts_value(opts, value):
    if opts == value:
        return True

    raise ValueError(f'{opts} must be {value}')


def validate_opts_type(opts, type_):
    if isinstance(opts, type_):
        return True
    raise TypeError(f'{opts} should be {type_}')


def validate_all_true(opts:list):
    if all(opts):
        return True
    raise ValueError('not all opts are true')

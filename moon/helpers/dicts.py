from datetime import datetime, date


def normalize(data):
    """
    Removes special chars from dict values
    :param data: Dict
    :return: A dict without special chars
    """
    for key in data:
        if isinstance(data[key], str):
            data[key] = data[key].replace('\'', '')
        if isinstance(data[key], datetime) or isinstance(data[key], date):
            data[key] = str(data[key])

    return data

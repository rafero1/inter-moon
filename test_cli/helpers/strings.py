def get_values_string(params):
    rs = [f"{str(param)}" if isinstance(param, int)
          else f"'{param}'" for param in params]
    return ", ".join(rs)


def remove_special_chars(text):
    """
    Removes single quotes
    """
    text = text.replace('\'', '')
    return text


def add_single_quotes(string):
    return "'{0}'".format(string)


def truncate_string(str_data, max_size):
    return (str_data[:max_size] + '..') if len(str_data) > max_size else str_data

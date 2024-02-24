def get_values_string(params):
    """ Returns a string with the values of the parameters """
    rs = [f"{str(param)}" if isinstance(param, int)
          else f"'{param}'" for param in params]
    return ", ".join(rs)


def remove_special_chars(text):
    """
    Removes single quotes
    """
    text = text.replace('\'', '')
    return text


def truncate(text: str, length: int = 50) -> str:
    """
    Truncates a string to :length: characters
    """
    if len(text) > length:
        return text[:length] + '...'
    return text


def ellipsize(text: str, length: int = 50) -> str:
    """
    Truncates a string to :length: characters
    """
    if len(text) > length:
        return text[:length] + '...' + text[-length:]
    return text


def add_single_quotes(string):
    """ Adds single quotes to a string """
    return f"'{string}'"

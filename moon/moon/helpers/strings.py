def get_values_string(params):
    rs = [f"{str(param)}" if isinstance(param, int)
          else f"'{param}'" for param in params]
    return ", ".join(rs)

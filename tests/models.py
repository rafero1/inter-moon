import typing
from strings import add_single_quotes as quote


class Attribute:
    def __init__(self, name: str, numeric: bool = False, attrtype: str = 'varchar') -> None:
        self.name = name
        self.numeric = numeric
        self.attrtype = attrtype

    def __str__(self) -> str:
        return self.name

    def get_value(self, value: str) -> str:
        return value if self.numeric else quote(value)

class Model:
    def __init__(self, table: str, attributes: typing.List[Attribute]) -> None:
        self.table = table
        self.attributes = attributes

    def add_attribute(self, attribute: Attribute) -> typing.Any:
        self.attributes.append(attribute)
        return self

    def get_attribute_names(self) -> str:
        return ",".join([str(attr) for attr in self.attributes])

    def get_create_table_basic(self) -> str:
        return ", ".join([f"{attr} {attr.attrtype}" for attr in self.attributes])

    def get_values(self, values: list) -> str:
        test = [self.attributes[i].get_value(val) for i, val in enumerate(values)]
        return ", ".join(test)

    def get_insert(self, values: list) -> str:
        return f"INSERT INTO {self.table} ({', '.join([str(attr) for attr in self.attributes])}) VALUES ({self.get_values(values)});"

    def get_multivalued_insert(self, values: list) -> str:
        return f"INSERT INTO {self.table} ({', '.join([str(attr) for attr in self.attributes])}) VALUES {', '.join([f'({self.get_values(val)})' for val in values])};"

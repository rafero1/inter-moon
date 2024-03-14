import typing
import pydash
from helpers.strings import add_single_quotes
from mapper.converters import to_bool, to_float, to_integer, to_varchar


class SQLColumnType:
    """
    Represents SQL column types.
    """
    VARCHAR = 'varchar'
    INTEGER = 'integer'
    DECIMAL = 'decimal'
    BOOL = 'bool'
    DATE = 'date'
    TIMESTAMP = 'timestamp'


class Column:
    """
    Represents an SQL column.
    """

    # callable functions to convert values to SQL format
    insert_value_functions = {
        SQLColumnType.VARCHAR: lambda x: add_single_quotes(to_varchar(x)),
        SQLColumnType.INTEGER: to_integer,
        SQLColumnType.DECIMAL: to_float,
        SQLColumnType.BOOL: to_bool,
        SQLColumnType.DATE: lambda x: add_single_quotes(to_varchar(x)),
        SQLColumnType.TIMESTAMP: lambda x: add_single_quotes(to_varchar(x))
    }

    def __init__(self, name: str, type: str):
        """
        Returns a new instance of a Column class.

        :param name str: The column name.
        :param type str: The colum type. Use @SQLColumnType statics to be sure.
        """
        self.name = name
        self.type = type
        self.insert_value_function = Column.insert_value_functions.get(
            type, lambda x: x)


class ColumnList(list):

    def __init__(self, columns) -> None:
        super().__init__(columns)

    def column_names(self) -> list[str]:
        """
        Returns a list of column names.
        """
        return [col.name for col in self]

    def column_types(self) -> list[str]:
        """
        Returns a list of column types.
        """
        return [col.type for col in self]


class Attribute():
    def __init__(self, column: Column, value) -> None:
        self.column = column
        self.value = value

    def get_insert_value(self) -> str:
        return 'NULL' if self.value is None else str(self.column.insert_value_function(self.value))


DEFAULT_ENTITY = "__entity"
DEFAULT_HASH = "__hash"
DEFAULT_PREVIOUS = "__previous"
DEFAULT_STATUS = "__status"

DEFAULT_OMITTED_COLUMNS = [DEFAULT_ENTITY,
                           DEFAULT_HASH, DEFAULT_PREVIOUS, DEFAULT_STATUS]

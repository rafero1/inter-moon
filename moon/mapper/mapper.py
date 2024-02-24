import typing
import sqlparse
import pydash
from datetime import datetime
from helpers.strings import add_single_quotes, ellipsize, remove_special_chars
from mapper.column import DEFAULT_ENTITY, DEFAULT_HASH, DEFAULT_OMITTED_COLUMNS, Attribute, Column, ColumnList, SQLColumnType
from mapper.converters import to_bool, to_float, to_integer, to_varchar
from mapper.schema_manager import SchemaManager
from timeit import default_timer as timer

from helpers.floats import s_to_ms


class Mapper:
    @staticmethod
    def sql_delete_index_entries(entity, hashes_list):
        """
        Generates a sql delete to indexes

        :param entity: Entity name
        :param hashes_list: Python list of blockchain hashes
        :return: SQL query to delete index entries
        """
        hashes_to_delete = str(hashes_list)
        hashes_to_delete = hashes_to_delete.replace('[', '(')
        hashes_to_delete = hashes_to_delete.replace(']', ')')
        sql_stmt = 'DELETE FROM {}_index WHERE bc_entry IN {}'.format(
            entity,
            hashes_to_delete
        )
        print("> prepared stmt:", ellipsize(sql_stmt))
        return sql_stmt

    @staticmethod
    def get_property_names_from_asset(bc_asset: dict, omit: list = DEFAULT_OMITTED_COLUMNS) -> typing.List[str]:
        """
        Returns a set of properties found in a blockchain asset in dict format.

        :param bc_asset dict: The blockchain asset in dict format.
        :param omit list: Keys to omit.

        :return: List of property names, except omitted.
        """

        return list(pydash.omit(bc_asset['data'], omit).keys())

    @staticmethod
    def get_columns_from_assets(entity_name: str, bc_data: typing.List[dict], omit: list = DEFAULT_OMITTED_COLUMNS) -> ColumnList:
        # attributes = pydash.chain(bc_data).map(lambda asset: Mapper.get_property_names_from_asset(asset, omit)).flatten().uniq().value()

        # schema manager-defined attributes for defining column types
        schema_attrs = SchemaManager.get_attributes_name_type_pair_by_entity(
            entity_name)

        # column_list = ColumnList([None] * len(attributes))
        column_list = ColumnList([None] * len(schema_attrs))

        # for i, attr in enumerate(attributes):
        #     column_list[i] = Column(attr, schema_attrs.get(attr, SQLColumnType.VARCHAR))
        for i, (col_name, col_type) in enumerate(schema_attrs.items()):
            column_list[i] = Column(col_name, col_type)

        return column_list

    @staticmethod
    def generate_sql_create_temp_table_from_columns(table_name: str, columns: ColumnList, with_hash: bool = False):
        """
        Generates an SQL query to create a temporary table
        based on attributes present in a list of columns

        :param table_name str: Name for the new temp table.
        :param columns ColumnList: List of table columns.
        :param with_hash bool: Whether to include the bc hash column.

        :return: SQL to create temp table.
        """

        # generate the column definiton for each attribute
        cols = ",".join(
            [f"{column.name} {column.type}" for column in columns])

        if with_hash:
            cols += f",{DEFAULT_HASH} varchar"

        return f'CREATE TEMP TABLE {table_name} ({cols})'

    @staticmethod
    def generate_sql_insert_values(asset: dict, columns: ColumnList) -> str:
        """
        Generates a list of properly formatted values (x) from a blockchain asset
        to be utilized in a SQL INSERT INTO table (columns) VALUES (x) query,
        based on the given list of columns.

        Only the properties corresponding to the given list of columns will pe pulled from the asset.

        :param asset dict: The blockchain asset whose properties will be turned into SQL values.
        :param columns ColumnList: The list of columns the query asks for.
        :return: A formatted list of values in string format.

        Example:
        >>> asset = {
            "name": "Michael",
            "age": 26,
            "job": None
        }

        Will be returned as:

        >>> "('Michael', 26, NULL)".
        """
        attrs = Mapper.get_attributes_from_asset(asset, columns)
        values = Mapper.get_value_list_from_attributes(attrs)
        return f"({','.join(values)})"

    @staticmethod
    def generate_sql_insert_into_temp_table_from_bc_data(table_name: str, columns: ColumnList, bc_data: typing.List[dict], with_hash: bool = False):
        """
        Generates an SQL query to insert blockchain tx data into a given table.

        :param table_name str: The table name.
        :param columns ColumnList: List of table columns.
        :param bc_data list[dict]: List of blockchain tx data to insert into the table.
        :param with_hash bool: Whether to include the bc hash column.

        :return: SQL to insert into a temp table
        """

        # bc_data must not be empty
        if not bc_data:
            return None

        # generate the column definiton for each attribute
        cols = ",".join(columns.column_names())

        if with_hash:
            cols += ',' + DEFAULT_HASH
            columns.append(Column(DEFAULT_HASH, SQLColumnType.VARCHAR))

        # start = timer()
        # values_list = [Mapper.generate_sql_insert_values(asset, columns) for asset in bc_data]

        values_list = (
            "(" + ",".join([
                attr.get_insert_value() for attr in [Attribute(col, bc_asset['data'].get(col.name, None)) for col in columns]
            ]) + ")" for bc_asset in bc_data)

        # end = timer()
        # print(">> finished generating values in", s_to_ms(end-start, 5), "ms")
        rs = f"INSERT INTO {table_name} ({cols}) VALUES {','.join(values_list)}"

        return rs

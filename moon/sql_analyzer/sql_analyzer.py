import typing
import sqlparse
import sys
from helpers.strings import add_single_quotes
from logger import log
from mapper.column import DEFAULT_ENTITY, DEFAULT_HASH


class SQLAnalyzer:
    """Contains a collection of helper methods to extract information from SQL statements.
    """

    def __init__(self, sql: str) -> None:
        """Creates a new instance of the SQL Analyzer.

        :param sql str: An SQL statement in string format.
        """
        self.sql = sql

    def get_conditonal(self) -> str:
        """
        Returns a string containing only the conditional part of a query, if any are found.

        :return str: A string containing only the conditional. Ex: "WHERE 1 = 1 AND 2 > 1".
        """
        parsed = sqlparse.parse(self.sql)
        tokens = parsed[0].tokens
        for token in tokens:
            if isinstance(token, sqlparse.sql.Where):
                return str(token)
        return ''

    def get_predicates_after_identifier(self) -> str:
        """
        Returns a string containing everything after the first identifier statement in a query.

        For UPDATE and DELETE, for example, the first identifier will be the table name (eg. UPDATE table_name SET... / DELETE FROM table_name...).

        :return str: A string containing the remainder of a query.
        """
        parsed = sqlparse.parse(self.sql)
        stmt = parsed[0]
        tokens = parsed[0].tokens

        # loop through tokens until we find the table identifier,
        # then parse every token after it into strings, join and return
        # i+2 because we skip the identifier itself and the following whitespace
        for i, token in enumerate(tokens):
            if isinstance(token, sqlparse.sql.Identifier):
                return "".join([str(token) for token in stmt.tokens[i+2:]])
        return ''

    def generate_select_from_identifier(self, table_identifier: str, select: str = '*', include_predicates: bool = True):
        """
        Generates a SELECT query from the base query, a given identifier and the chosen columns.

        :param table_identifier str: The table identifier.
        :param select str: The selected attributes.
        :param include_predicates str: Whether to bring the remainder of the original query into the SELECT or not.
        :return str: The query in string format.
        """

        predicates = self.get_predicates_after_identifier()
        if include_predicates and predicates:
            return f"SELECT {select} FROM {table_identifier}" + " " + predicates
        else:
            return f"SELECT {select} FROM {table_identifier}"

    def generate_select_all_where_in(self, table_identifier: str, collection: typing.List[str], conditional: str = DEFAULT_HASH):
        """
        Generates a SELECT {hash_column} query from the base query, a given identifier and the chosen columns.

        :param table_identifier str: The table identifier.
        :param select str: The selected attributes.
        :param bring_remainder str: Whether to bring the remainder of the original query into the SELECT or not.
        :return str: The query in string format.
        """

        return f"SELECT * FROM {table_identifier} WHERE {conditional} IN ({','.join(collection)})"

    def update_to_select(self):
        parsed = sqlparse.parse(self.sql)
        stmt = parsed[0]
        tokens = parsed[0].tokens

        # loop through tokens until we find the table identifier,
        # then parse every token after it into strings, join and return
        for i, token in enumerate(tokens):
            if isinstance(token, sqlparse.sql.Identifier):
                return "".join([str(token) for token in stmt.tokens[-i:]])
        return ''

    def insert_to_dict(self, entity_name=None):
        """
        Map SQL INSERT-type queries to dicts.

        Supported query format:
        INSERT INTO table_name (col1,col2...) VALUES (val1,val2...)

        :param query str: The SQL query in string format.
        :param entity_name str: The asset name. Defaults to the query table identifier when None.
        :return list[dict]: A list of dicts representing a collection of values.
        """

        res = sqlparse.parse(self.sql)
        stmt = res[0]
        tokens = stmt.tokens

        rs = []
        keys = [DEFAULT_ENTITY]

        try:
            for i, token in enumerate(tokens):
                if not isinstance(token, sqlparse.sql.Token):
                    raise ValueError

                if i == 0 and token.value.lower() != 'insert':
                    raise ValueError

                if i == 2 and token.value.lower() != 'into':
                    raise ValueError

                if i == 4:  # must be table_name (col1,col2,col3,...)
                    if not isinstance(token, sqlparse.sql.Function):
                        raise ValueError

                    # add table column names into key list to use as dict keys
                    keys.extend(
                        [param.value for param in token.get_parameters()])

                    # set entity name to table_name if none was provided
                    if entity_name is None or not entity_name or entity_name.isspace():
                        entity_name = token.get_name()

                if i == 6:  # must be VALUES (...)
                    if not isinstance(token, sqlparse.sql.Values):
                        raise ValueError

                    # iterate over groups of values
                    for sublist in token.get_sublists():
                        insert_values = sublist[1]

                        # iterate over values inside group and add each valid value to a list
                        values = []
                        if isinstance(insert_values, sqlparse.sql.TokenList):
                            values = [
                                val.value for val in insert_values if val.value != ',' and val.value != ' ']
                            values.insert(0, entity_name)

                        # create a dict with the table column names as keys
                        # and the tokens from the value list as values
                        rs.append(
                            {key: value for key, value in zip(keys, values)})

            return rs
        except Exception as e:
            log.e('SQL Analyzer', sys.exc_info())

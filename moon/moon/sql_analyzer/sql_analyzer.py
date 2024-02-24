import sqlparse


class SQLAnalyzer:
    """Contains a collection of helper methods to extract information from SQL statements.
    """

    def __init__(self, sql: str) -> None:
        """Creates a new instance of the SQL Analyzer.

        :param sql: An SQL statement in string format.
        """
        self.sql = sql

    def sql_get_conditonal(self) -> str:
        """
        Returns a string containing only the conditional part of a query, if any are found.

        :return: A string containing only the conditional. Ex: "WHERE 1 = 1 AND 2 > 1".
        """
        parsed = sqlparse.parse(self.sql)
        tokens = parsed[0].tokens
        for token in tokens:
            if isinstance(token, sqlparse.sql.Where):
                return token
        return ''

    def get_remainder_after_delete(self) -> str:
        """
        Returns a string containing everything after the DELETE FROM statement in a query.

        :return: A string containing the remainder of a DELETE query.
        """
        parsed = sqlparse.parse(self.sql)
        stmt = parsed[0]
        tokens = parsed[0].tokens

        # loop through tokens until we find the table identifier,
        # then parse every token after it into strings, join and return
        for i, token in enumerate(tokens):
            if isinstance(token, sqlparse.sql.Identifier):
                return "".join([str(token) for token in stmt.tokens[-i:]])
        return ''

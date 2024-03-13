from logger import log
import sqlparse
from sql_metadata import Parser
from communication.query_type import DELETE, INSERT, SELECT, UPDATE
from sqlparse import sql as SQL
from sqlparse import tokens as T


class QueryAnalyzer:
    JOINS = ('join', 'inner join', 'outer join', 'cross join', 'full join',
             'left join', 'right join', 'left outer join', 'right outer join')

    @staticmethod
    def get_type_query(query):
        """
        Recognizes the type of operation of the SQL request
        :param query: A SQL query
        :return: The operation type(INSERT, UPDATE, SELECT or DELETE)
        """
        stmt_list = sqlparse.parse(query)
        first_token = str(stmt_list[0].tokens[0]).lower()

        if first_token == 'select':
            return SELECT
        elif first_token == 'update':
            return UPDATE
        elif first_token == 'delete':
            return DELETE
        elif first_token == 'insert':
            return INSERT
        else:
            raise Exception(
                'Unrecognized Operation in Query \'{}\''.format(query)
            )

    @staticmethod
    def get_entities_parser(query: str):
        """
        Get involved entities involved in a query
        :param query: Sql query
        :return: A list with the involved entities
        """
        identifiers = Parser(query).tables
        return identifiers

    @staticmethod
    def get_entities(query: str):
        """
        Get involved entities involved in a query
        :param query: Sql query
        :return: A list with the involved entities
        """
        stmt_list = sqlparse.parse(query)
        stmt = stmt_list[0]
        tokens = stmt.tokens

        identifiers = set()

        def rec_get_identifiers(tokens, identifiers):
            # log.i(
            #     'SQL Analyzer',
            #     f"Tokens: {tokens}",
            # )

            # loop through tokens until we find any table names
            for i, token in enumerate(tokens):
                previous_keyword = str(tokens[i-2]).lower()

                # if the current token is a list of identifiers, it is a list of tables
                if isinstance(token, SQL.IdentifierList):
                    if previous_keyword == 'from' or previous_keyword in QueryAnalyzer.JOINS:
                        for identifier in token.get_identifiers():
                            identifiers.add(identifier.get_real_name())

                # if it is an identifier and it is after FROM, UPDATE or JOIN, it could be a table
                elif isinstance(token, sqlparse.sql.Identifier):
                    if previous_keyword in ('from', 'update') or previous_keyword in QueryAnalyzer.JOINS:
                        first_child = token.token_first()
                        if first_child is not None and first_child.ttype == T.Name:
                            identifiers.add(token.get_real_name())

                    # if it is a subquery, recurse
                    for t in token.tokens:
                        if isinstance(t, sqlparse.sql.Parenthesis):
                            rec_get_identifiers(t.tokens, identifiers)

                # if it is a function, it could be a table name after INSERT INTO
                elif isinstance(token, SQL.Function) and previous_keyword in ('into'):
                    identifiers.add(token.get_real_name())

                # if the token is a WHERE, a comparison (inside a WHERE) or a subquery, recurse
                elif isinstance(token, (SQL.Where, SQL.Comparison, SQL.Parenthesis)):
                    rec_get_identifiers(token.tokens, identifiers)

        rec_get_identifiers(tokens, identifiers)

        log.i(
            'SQL Analyzer',
            f"Identifiers: {identifiers}",
        )

        return list(identifiers)

    @staticmethod
    def has_conditional(query):
        """
        Check if the select has a conditionals
        :param query: A select SQL query
        :return: True if the query has a conditionals, or false otherwise
        """
        if 'where' in query.lower():
            return True
        return False

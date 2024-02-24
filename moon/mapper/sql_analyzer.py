import sqlparse
from sql_metadata import Parser
from communication.query_type import DELETE, INSERT, SELECT, UPDATE
from sqlparse import sql as SQL
from sqlparse import tokens as T

class QueryAnalyzer:
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

        identifiers = []

        def rec_get_identifiers(tokens, identifiers):
            # print(tokens)
            for i, token in enumerate(tokens):
                # if the token is a list of identifiers, add them all
                if isinstance(token, SQL.IdentifierList) and str(tokens[i-2]).lower() in ('from', 'join'):
                    for t in token.get_identifiers():
                        if t.get_real_name() not in identifiers:
                            identifiers.append(t.get_real_name())
                # if the token is an identifier, it could be a subquery, a JOIN or a FROM
                elif isinstance(token, sqlparse.sql.Identifier):
                    # if the token comes after a FROM or JOIN, it could be a table name
                    if str(tokens[i-2]).lower() == 'from' or 'join' in str(tokens[i-2]).lower():
                        if token.token_first().ttype == T.Name:
                            if token.get_real_name() not in identifiers:
                                identifiers.append(token.get_real_name())
                    # if it is a subquery, recurse
                    for t in token.tokens:
                        if isinstance(t, sqlparse.sql.Parenthesis):
                            rec_get_identifiers(t.tokens, identifiers)
                # if the token is a WHERE, a comparison (inside a WHERE) or a subquery, recurse
                elif isinstance(token, SQL.Where) or isinstance(token, SQL.Comparison) or isinstance(token, SQL.Parenthesis):
                    rec_get_identifiers(token.tokens, identifiers)

        rec_get_identifiers(tokens, identifiers)

        return identifiers

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

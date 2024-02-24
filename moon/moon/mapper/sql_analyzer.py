import sqlparse
from moon.communication.query_type import DELETE, INSERT, SELECT, UPDATE


class QueryAnalyzer:
    @staticmethod
    def get_type_query(query):
        """
        Recognizes the type of operation of the SQL request
        :param query: A SQL query
        :return: The operation type(INSERT, UPDATE, SELECT or DELETE)
        """
        res = sqlparse.parse(query)
        stmt = res[0]
        tokens = stmt.tokens
        if str(tokens[0]).lower() == 'select':
            return SELECT
        elif str(tokens[0]).lower() == 'update':
            return UPDATE
        elif str(tokens[0]).lower() == 'delete':
            return DELETE
        elif str(tokens[0]).lower() == 'insert':
            return INSERT
        else:
            raise Exception(
                'Unrecognized Operation in Query \'{}\''.format(query)
            )

    @staticmethod
    def get_involved_entities(type_query, query):
        """
        Get involved entities involved in a query
        :param type: Type of query (SELECT, INSERT, UPDATE or DELETE)
        :param query: Sql query
        :return: A list with the involved entities
        """
        if type_query == SELECT:
            res = sqlparse.parse(query)
            stmt = res[0]
            tokens = stmt.tokens
            count = 0
            entities = ''
            for t in tokens:
                if str(t).lower() == 'from':
                    count += 1
                    continue
                if count in [1, 2]:
                    entities = entities + str(t).replace(' ', '')
                    count += 1
            entities = entities.split(',')
            return entities
        elif type_query == UPDATE:
            res = sqlparse.parse(query)
            stmt = res[0]
            tokens = stmt.tokens
            entity = tokens[2]
            return [str(entity)]
        elif type_query == DELETE:
            res = sqlparse.parse(query)
            stmt = res[0]
            tokens = stmt.tokens
            entity = tokens[4]
            return [str(entity)]
        elif type_query == INSERT:
            res = sqlparse.parse(query)
            stmt = res[0]
            tokens = stmt.tokens
            entity = str(tokens[4]).split(' ')[0]
            return [entity]
        else:
            raise Exception(
                'Unrecognized Operation in Query \'{}\''.format(query)
            )

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

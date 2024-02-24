class NewRequest:
    """
    MOON wrapper for SQL requests to be made to either the Blockchain or Relational systems.
    """
    INSERT = 0
    UPDATE = 1
    DELETE = 2
    SELECT = 3

    def __init__(self, q_query):
        self.q_query = q_query
        self.q_type = None
        self.q_entities = None
        self.db_name = None

    def __str__(self):
        dict_return = dict()
        dict_return['q_query'] = self.q_query
        dict_return['q_type'] = self.q_type
        dict_return['q_entities'] = self.q_entities
        dict_return['db_name'] = self.db_name
        return str(dict_return)

    @staticmethod
    def make_by_dict(data: dict):
        request = NewRequest(data['q_query'])
        request.q_type = data['q_type']
        request.q_entities = data['q_entities']
        request.db_name = data['db_name']
        return request

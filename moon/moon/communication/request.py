class Request:
    INSERT = 0
    UPDATE = 1
    DELETE = 2
    SELECT = 3

    def __init__(
        self, q_query, db_user, db_name,
        db_password, db_port, bc_port,
        db_host, bc_host, bc_public_key,
        bc_private_key, q_type, q_entities
    ):
        self.q_query = q_query
        self.q_type = q_type
        self.q_entities = q_entities
        self.db_user = db_user
        self.db_name = db_name
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.bc_host = bc_host
        self.bc_port = bc_port
        self.bc_public_key = bc_public_key
        self.bc_private_key = bc_private_key

    def __str__(self):
        dict_return = dict()
        dict_return['q_query'] = self.q_query
        dict_return['q_type'] = self.q_type
        dict_return['q_entities'] = self.q_entities
        dict_return['db_user'] = self.db_user
        dict_return['db_name'] = self.db_name
        dict_return['db_password'] = self.db_password
        dict_return['db_host'] = self.db_host
        dict_return['db_port'] = self.db_port
        dict_return['bc_host'] = self.bc_host
        dict_return['bc_port'] = self.bc_port
        dict_return['bc_public_key'] = self.bc_public_key
        dict_return['bc_private_key'] = self.bc_private_key
        return str(dict_return)

    @staticmethod
    def make_by_dict(data):
        request = Request(
            q_query=data['q_query'],
            db_user=data['db_user'],
            db_name=data['db_name'],
            db_password=data['db_password'],
            db_port=data['db_port'],
            bc_port=data['bc_port'],
            db_host=data['db_host'],
            bc_host=data['bc_host'],
            bc_public_key=data['bc_public_key'],
            bc_private_key=data['bc_private_key'],
            q_type=data['q_type'],
            q_entities=data['q_entities']
        )
        return request

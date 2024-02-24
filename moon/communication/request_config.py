class RequestConfig:
    def __init__(self, db_user, db_name, db_password, db_port, bc_port, db_host, bc_host, bc_public_key, bc_private_key) -> None:
        self.db_user = db_user
        self.db_name = db_name
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.bc_host = bc_host
        self.bc_port = bc_port
        self.bc_public_key = bc_public_key
        self.bc_private_key = bc_private_key

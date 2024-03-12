class RequestConfig:
    def __init__(self, db_user, db_name, db_password, db_port, db_host) -> None:
        self.db_user = db_user
        self.db_name = db_name
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

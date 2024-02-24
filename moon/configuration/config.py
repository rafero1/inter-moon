import os
import dotenv


class Configuration:
    __instance = None

    moon_host = None
    moon_port = None
    use_mongo = None
    mongo_string = None
    sql_adapter = None
    db_user = None
    db_name = None
    db_password = None
    db_port = None
    bc_port = None
    bc_host = None
    db_host = None
    bc_public_key = None
    bc_private_key = None
    bc_index_dbname = None

    def __init__(self):
        if Configuration.__instance is not None:
            raise Exception("Singleton class, use get_instance() method to get instance")
        else:
            Configuration.__instance = self
            self.__load_config()

    @staticmethod
    def get_instance() -> 'Configuration':
        if Configuration.__instance is None:
            Configuration()
        return Configuration.__instance

    def __load_config(self):
        dotenv.load_dotenv(dotenv.find_dotenv())
        self.moon_host = os.getenv('MOON_HOST')
        self.moon_port = os.getenv('MOON_PORT')
        self.use_mongo = os.getenv('USE_MONGO')
        self.mongo_string = os.getenv('MONGO_STRING')
        self.sql_adapter = os.getenv('SQL_ADAPTER')
        self.db_user = os.getenv('DB_USER')
        self.db_name = os.getenv('DB_NAME')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_port = os.getenv('DB_PORT')
        self.bc_port = os.getenv('BC_PORT')
        self.bc_host = os.getenv('BC_HOST')
        self.db_host = os.getenv('DB_HOST')
        self.bc_public_key = os.getenv('BC_PUBLIC_KEY')
        self.bc_private_key = os.getenv('BC_PRIVATE_KEY')
        self.bc_index_dbname = os.getenv('BC_INDEX_DBNAME')

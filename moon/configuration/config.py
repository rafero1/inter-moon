import os
import dotenv


class Configuration:
    __instance = None

    moon_host = None
    moon_port = None

    sql_adapter = None
    db_user = None
    db_name = None
    db_password = None
    db_port = None
    db_host = None
    bc_index_dbname = None

    network_profile_path = None
    requester_org = None
    requester_name = None
    peers = None
    channel = None
    cc_name = None


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
        return Configuration.__instance or Configuration()

    def __load_config(self):
        dotenv.load_dotenv(dotenv.find_dotenv())
        self.moon_host = os.getenv('MOON_HOST')
        self.moon_port = os.getenv('MOON_PORT')

        self.sql_adapter = os.getenv('SQL_ADAPTER')
        self.db_user = os.getenv('DB_USER')
        self.db_name = os.getenv('DB_NAME')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_port = os.getenv('DB_PORT')
        self.db_host = os.getenv('DB_HOST')
        self.bc_index_dbname = os.getenv('BC_INDEX_DBNAME')

        self.network_profile_path = os.getenv('NETWORK_PROFILE_PATH')
        self.requester_org = os.getenv('REQUESTER_ORG')
        self.requester_name = os.getenv('REQUESTER_NAME')
        self.peers = str(os.getenv('PEERS')).split(',')
        self.channel = os.getenv('CHANNEL')
        self.cc_name = os.getenv('CC_NAME')

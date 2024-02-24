import psycopg2

class GenericDBDriver:
    __instance = None
    adapter = psycopg2

    def __init__(self):
        if GenericDBDriver.__instance is not None:
            raise Exception("Singleton class, use get_instance() method to get instance")
        else:
            GenericDBDriver.__instance = self

    @staticmethod
    def get_instance() -> 'GenericDBDriver':
        if GenericDBDriver.__instance is None:
            GenericDBDriver()
        return GenericDBDriver.__instance

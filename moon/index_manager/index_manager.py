from helpers.floats import s_to_ms
from configuration.config import Configuration
from sqlclient.clientsql import ClientSQL
from communication.request import Request
from mapper.schema_manager import SchemaManager
from communication.query_type import INSERT, SELECT
from timeit import default_timer as timer


class IndexManager:

    @staticmethod
    def get_ids_by_entity(request, entity):
        """
        Gets the ID of each data of an
        entity that is on the Blockchain
        :param request: Request information
        :param entity: The entity name
        :return: List of data IDs
        """
        config = Configuration.get_instance()
        start = timer()

        query_get_index = f"SELECT bc_entry FROM {entity}_index"
        req = Request(
            q_query=query_get_index,
            q_type=SELECT,
            q_entities=[f"{entity}_index"],
            db_port=config.db_port,
            db_host=config.db_host,
            db_name=config.bc_index_dbname,
            db_password=config.db_password,
            db_user=config.db_user,
            bc_host=config.bc_host,
            bc_port=config.bc_port,
            bc_private_key=config.bc_private_key,
            bc_public_key=config.bc_public_key
        )

        sql_client = ClientSQL(request=req)
        sql_client.start()
        sql_client.join()
        result = sql_client.get_result()
        list_assets_ids = []

        for r in result:
            list_assets_ids.append(r[0])

        end = timer()

        print('finished retrieving index entries in',
              s_to_ms(end-start, 5), 'ms')
        return list_assets_ids

    @staticmethod
    def store_index(entity, bc_id, rbd_id):
        """
        Stores an index entry for a newly recorded
        transaction on the blockchain
        :param entity: The entity name
        :param bc_id: The blockchain transaction asset id
        :param rbd_id: The RBD primary key
        """
        config = Configuration.get_instance()

        sql_store_index = f"INSERT INTO {entity}_index (id, bc_entry) VALUES ({rbd_id}, {bc_id})"
        req = Request(
            q_query=sql_store_index,
            q_type=INSERT,
            q_entities=[f"{entity}_index"],
            db_port=config.db_port,
            db_host=config.db_host,
            db_name=config.bc_index_dbname,
            db_password=config.db_password,
            db_user=config.db_user,
            bc_host=config.bc_host,
            bc_port=config.bc_port,
            bc_private_key=config.bc_private_key,
            bc_public_key=config.bc_public_key)
        sql_client = ClientSQL(request=req)
        sql_client.start()
        sql_client.join()

import sys
import copy
from moon.logger import log
from threading import Thread
from moon.mapper.mapper import Mapper
from bigchaindb_driver import BigchainDB
from moon.sql_analyzer.sql_analyzer import SQLAnalyzer
from moon.sqlclient.clientsql import ClientSQL
from moon.communication.request import Request
from moon.mapper.schema_manager import SchemaManager
from moon.mapper.persistence_model import BLOCKCHAIN
from bigchaindb_driver.crypto import generate_keypair
from moon.index_manager.index_manager import IndexManager
from moon.sqlclient.data_temp_manager import DataTempManager
from moon.communication.query_type import DELETE, INSERT, SELECT, UPDATE
from timeit import default_timer as timer


class ClientBlockchain(Thread):
    def __init__(self, request):
        super().__init__()
        self.result = None
        self.request = request
        self.q_query = request.q_query
        self.q_type = request.q_type
        self.q_entities = request.q_entities
        self.db_user = request.db_user
        self.db_name = request.db_name
        self.db_password = request.db_password
        self.db_host = request.db_host
        self.db_port = request.db_port
        self.bc_host = request.bc_host
        self.bc_port = request.bc_port
        self.bc_public_key = request.bc_public_key
        self.bc_private_key = request.bc_private_key

    def run(self):
        if SELECT == self.q_type:
            print('starting SELECT operation...')
            start = timer()
            self.result = self._select_blockchain()
            end = timer()
            print('finished SELECT in', round(end-start, 5), 'ms')
        elif INSERT == self.q_type:
            print('starting INSERT operation...')
            start = timer()
            self.result = self._insert_blockchain()
            end = timer()
            print('finished INSERT in', round(end-start, 5), 'ms')
        elif UPDATE == self.q_type:
            print('starting UPDATE operation...')
            start = timer()
            self.result = self._update_blockchain()
            end = timer()
            print('finished UPDATE in', round(end-start, 5), 'ms')
        elif DELETE == self.q_type:
            print('starting DELETE operation...')
            start = timer()
            self.result = self._delete_blockchain()
            end = timer()
            print('finished DELETE in', round(end-start, 5), 'ms')
        else:
            raise Exception('Unrecognized Request Type')

    def _insert_blockchain(self):
        """
        Executes INSERTs into blockchain
        :return: Number of affected rows
        """
        data_transaction = Mapper.sql_insert_to_bc(
            self.q_query,
            self.q_entities[0]
        )
        return self._persist_data_bc(data_transaction, self.q_entities[0])

    def _persist_data_bc(self, data, entity):
        """
        Stores data on Blockchain
        :param data: List of dicts. It will be persisted
        :return: Number of affected rows
        """
        bdb = BigchainDB(
            'http://{}:{}'.format(self.bc_host, self.bc_port)
        )
        keypair = generate_keypair()
        try:
            for d in data:
                tx = bdb.transactions.prepare(
                    operation='CREATE',
                    signers=keypair.public_key,
                    asset={'data': self._remove_special_chars(d)}
                )
                signed_tx = bdb.transactions.fulfill(
                    tx,
                    private_keys=keypair.private_key
                )
                transaction_sent = bdb.transactions.send_sync(signed_tx)
                IndexManager.store_index(
                    self.request,
                    transaction_sent,
                    entity
                )
        except Exception:
            log.e(
                'Blockchain Client Module',
                sys.exc_info()
            )
            return 0
        return len(data)

    def _update_blockchain(self):
        """
        Executes an update in blockchain
        :return: Number of affected rows
        """
        sql_query_select_bc = Mapper.sql_update_to_bc(self.q_query)
        all_entity_data = self._get_data_bc(self.q_entities)
        new_data_to_update_bc = DataTempManager.update_bc_in_rdb(
            self.request,
            self.q_query,
            sql_query_select_bc,
            all_entity_data,
            self.q_entities[0]
        )

        if len(new_data_to_update_bc) == 0:
            return 0
        else:
            # List with index entries to delete,
            # Because it will be a out of date
            index_entries_to_delete = []

            data_to_insert_bc = []
            entity_keys = SchemaManager.get_entity_names_attributes(
                self.q_entities[0]
            )
            entity_keys.insert(0, 'entity')
            entity_keys.append('before')
            for reg in new_data_to_update_bc:
                reg = list(reg)
                reg.insert(0, self.q_entities[0])
                data_add = dict(zip(entity_keys, reg))
                index_entries_to_delete.append(data_add['before'])
                data_to_insert_bc.append(data_add)
            status = self._persist_data_bc(
                data_to_insert_bc,
                self.q_entities[0]
            )
            if status > 0:
                sql_delete_index_entries = Mapper.get_sql_delete_index_entries(
                    self.q_entities[0],
                    index_entries_to_delete
                )
                request_delete_index = Request(
                    q_query=sql_delete_index_entries,
                    db_user=self.db_user,
                    db_name='index_bc',
                    db_password=self.db_password,
                    db_port=self.db_port,
                    bc_port=self.bc_port,
                    db_host=self.db_host,
                    bc_host=self.bc_host,
                    bc_public_key=self.bc_public_key,
                    bc_private_key=self.bc_private_key,
                    q_type=DELETE,
                    q_entities=[self.q_entities[0] + '_index']
                )
                client_sql = ClientSQL(request_delete_index)
                client_sql.start()
                client_sql.join()
                result = client_sql.get_result()
                if result == status:
                    return status
                else:
                    return 0
            else:
                return 0

    def _delete_blockchain(self):
        """
        Executes a DELETE query for blockchain assets.

        1. find assets with name X fulfilling Y conditional in the blockchain (Z = SELECT * FROM X WHERE Y ...)
        2. get the bc index of every entry in Z
        3. delete rows from the index table of X which correspond to each index from Z

        :return: Number of deleted rows
        """

        # 1. find assets with name X fulfilling Y conditional in the blockchain (Z = SELECT * FROM X WHERE Y ...)
        bc_data = []
        bc_entities = []
        for entity in self.q_entities:
            if SchemaManager.get_entity_persistence(entity) == BLOCKCHAIN:
                bc_entities.append(entity)
                data_aux = self._get_data_bc([entity])
                bc_data.append(data_aux[0])

        # make a SELECT query using the input query tokens, other than DELETE,
        # to a temp table in order to get the specific list of hashes we want
        # to delete
        select = f"SELECT * FROM {self.q_entities[0]}"
        remainder = SQLAnalyzer(self.q_query).get_remainder_after_delete()
        if remainder:
            select += f" {remainder}"

        # 2. get the bc index of every entry in Z
        bc_hash_list = DataTempManager.select_hashes_with_conditionals_in_rdb(
            self.request,
            bc_data,
            select,
            bc_entities
        )

        if bc_hash_list == 0:
            return 0

        bc_hash_list = bc_hash_list[:, 2].tolist()

        # 3. delete rows from the index table of X which correspond to each index from Z
        sql_delete_index_entries = Mapper.get_sql_delete_index_entries(
            self.q_entities[0],
            bc_hash_list
        )
        request_delete_index = Request(
            q_query=sql_delete_index_entries,
            db_user=self.db_user,
            db_name='index_bc',
            db_password=self.db_password,
            db_port=self.db_port,
            bc_port=self.bc_port,
            db_host=self.db_host,
            bc_host=self.bc_host,
            bc_public_key=self.bc_public_key,
            bc_private_key=self.bc_private_key,
            q_type=DELETE,
            q_entities=[self.q_entities[0] + '_index']
        )
        client_sql = ClientSQL(request_delete_index)
        client_sql.start()
        client_sql.join()
        return client_sql.get_result()

    def _select_blockchain(self):
        """
        Executes a SELECT query for blockchain assets.

        Creates a temp table using the requested entities, executes the SELECT there and returns results.

        :return: List of tuples containing the query results.
        """
        bc_data = []
        bc_entities = []

        for entity in self.q_entities:
            if SchemaManager.get_entity_persistence(entity) == BLOCKCHAIN:
                bc_entities.append(entity)
                data_aux = self._get_data_bc([entity])
                bc_data.append(data_aux[0])

        return DataTempManager.select_with_conditionals_in_rdb(
            self.request,
            bc_data,
            self.q_query,
            bc_entities
        )

    def _get_data_bc(self, list_entities):
        """
        :param list_entities: Desired entities --> list format
        :return: Tuple with hash_code list and data list
        """
        data_hashes = []
        for entity in list_entities:
            data_hashes.append(
                IndexManager.get_ids_by_entity(self.request, entity)
            )
        data_per_entity = []
        bdb = BigchainDB(
            'http://{}:{}'.format(self.bc_host, self.bc_port)
        )

        start = timer()

        label = 0
        for entity_ids in data_hashes:
            data_per_entity.append([])
            for id_element in entity_ids:
                tx = bdb.transactions.get(asset_id=id_element)
                if tx != []:
                    data_instance = tx[0]['asset']['data']
                    data_instance['hash'] = id_element
                    data_per_entity[label].append(data_instance)
            label += 1

        end = timer()
        print('finished retrieving data in', round(end - start, 5), 'ms')

        return data_per_entity

    @staticmethod
    def _merge_two_dicts(x, y):
        """
        If the first and the second has the same key,
        the second overwrites the value of the first
        :param x: dict 1
        :param y: dict 2
        :return: a merged dict
        """
        z = copy.deepcopy(x)
        z.update(y)
        return z

    @staticmethod
    def _get_values_without_specific_keys(d, invalid_keys):
        """
        Gets a dict and remove specific keys,
        returns only the values
        :param d: a dict
        :param keys: the keys list
        :return: a dict without a listed keys
        """
        result = []
        for key in d.keys():
            if key not in invalid_keys:
                result.append(d[key])
        return result

    def get_result(self, final_result=True):
        '''
        Get result of request
        :param final_result: if its necessary
            to convert the result data to tuple format
        :return:
        '''
        # if self.q_type == SELECT and
        # final_result == True and len(self.result) != 0:
        #     # the aux will receive all
        # result data, this is not separated by entity
        #     aux = []
        #     result_to_return = []
        #     for list_entity in self.result:
        #         for data in list_entity:
        #             aux.append(data)
        #
        #     # removing 'entity' and 'hash' keys
        #     for data in aux:
        #         try:
        #             del data["entity"]
        #             del data["hash"]
        #         except KeyError:
        #             pass
        #         data = tuple(data.values())
        #         result_to_return.append(data)
        # else:
        #     result_to_return = self.result
        # # return result_to_return
        return self.result

    def _remove_special_chars(self, data):
        """
        Removes special chars from dict values
        :param data: Dict
        :return: A dict without special chars
        """
        for key in data:
            if isinstance(data[key], str):
                data[key] = data[key].replace('\'', '')
        return data

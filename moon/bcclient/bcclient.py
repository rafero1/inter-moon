import sys
import copy
import typing
from helpers.strings import truncate
import pydash
from logger import log
from threading import Thread
from mapper.column import DEFAULT_ENTITY, DEFAULT_HASH, DEFAULT_PREVIOUS
from mapper.mapper import Mapper
from bigchaindb_driver import BigchainDB
from configuration.config import Configuration
from sql_analyzer.sql_analyzer import SQLAnalyzer
from sqlclient.clientsql import ClientSQL
from communication.request import Request
from mapper.schema_manager import SchemaManager
from mapper.persistence_model import BLOCKCHAIN
from bigchaindb_driver.crypto import generate_keypair
from index_manager.index_manager import IndexManager
from sqlclient.data_temp_manager import DataTempManager
from communication.query_type import DELETE, INSERT, SELECT, UPDATE
from timeit import default_timer as timer
from helpers.floats import s_to_ms
import numpy as np
from pymongo.mongo_client import MongoClient
import datetime
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class ClientBlockchain(Thread):
    use_mongo = bool(os.environ['USE_MONGO'])
    mongo_string = os.environ['MONGO_STRING']

    def __init__(self, request):
        super().__init__()
        self.result = None
        self.request = request

    def run(self):
        if SELECT == self.request.q_type:
            print('>> starting SELECT operation...')
            start = timer()
            self.result = self._select_blockchain()
            end = timer()
            print('>> finished SELECT in', s_to_ms(end-start, 5), 'ms')
        elif INSERT == self.request.q_type:
            print('>> starting INSERT operation...')
            start = timer()
            self.result = self._insert_blockchain()
            end = timer()
            print('>> finished INSERT in', s_to_ms(end-start, 5), 'ms')
        elif UPDATE == self.request.q_type:
            print('>> starting UPDATE operation...')
            start = timer()
            self.result = self._update_blockchain()
            end = timer()
            print('>> finished UPDATE in', s_to_ms(end-start, 5), 'ms')
        elif DELETE == self.request.q_type:
            print('>> starting DELETE operation...')
            start = timer()
            self.result = self._delete_blockchain()
            end = timer()
            print('>> finished DELETE in', s_to_ms(end-start, 5), 'ms')
        else:
            raise Exception('Unrecognized Request Type')

    def _persist_data_bc(self, data, entity):
        """
        Stores data on Blockchain
        :param data: List of dicts. It will be persisted
        :return: Number of affected rows
        """
        config = Configuration.get_instance()

        bdb = BigchainDB(
            'http://{}:{}'.format(config.bc_host, config.bc_port)
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

    def _get_bc_data(self, *entities) -> typing.Iterable[dict]:
        """
        Returns a formatted list of blockchain transaction data for the given list of entities.

        :param entities: Entities with which to query blockchain tx data.
        :return: A list of dicts of blockchain tx data.
        """
        config = Configuration.get_instance()

        start = timer()

        # start = timer()
        # get a list of lists of blockchain asset ids, separated by asset type
        entities_asset_ids = [IndexManager.get_ids_by_entity(
            self.request, entity) for entity in entities]
        # end = timer()
        # print('>> finished getting asset ids in', s_to_ms(end-start, 5), 'ms')

        result_list = []

        if ClientBlockchain.use_mongo:
            asset_ids = pydash.flatten(entities_asset_ids)

            # start = timer()
            db: MongoClient = MongoClient(
                f"{ClientBlockchain.mongo_string}", connect=False)
            # end = timer()
            # print('>> connected to MongoDB in', s_to_ms(end-start, 5), 'ms')

            # TODO: batch-loading assets
            # start = timer()
            assets = db['bigchain']['assets'].find({"id": {"$in": asset_ids}}, {"id": 1, "data": 1})
            # end = timer()
            # print('>> finished querying MongoDB in', s_to_ms(end-start, 5), 'ms')

            # TODO: optimize this
            # start = timer()
            # for asset in assets:
            #     # print(truncate(asset))
            #     asset_data = asset['data']
            #     asset_data[DEFAULT_HASH] = asset['id']
            #     result_list.append(asset_data)
            # result_list = [dict(**asset['data'], DEFAULT_HASH = asset['id']) for asset in assets]
            result_list = list(assets)
            # end = timer()
            # print('>> finished formatting data in', s_to_ms(end-start, 5), 'ms')
        else:
            bdb = BigchainDB(
                'http://{}:{}'.format(config.bc_host, config.bc_port))

            for asset_id in pydash.flatten(entities_asset_ids):
                try:
                    tx = bdb.transactions.retrieve(asset_id)
                except (Exception):
                    tx = None

                if tx:
                    tx_data = tx['asset']['data']
                    tx_data[DEFAULT_HASH] = asset_id
                    result_list.append(tx_data)

        end = timer()
        print('>> finished retrieving data in', s_to_ms(end-start, 5), 'ms')

        return result_list

    def _select_blockchain(self):
        """
        Executes a SELECT query for blockchain assets.

        Creates a temp table using the requested entities, executes the SELECT there and returns results.

        :return: List of tuples containing the query results.
        """
        bc_data = []
        bc_entities = []

        for entity in self.request.q_entities:
            if SchemaManager.get_entity_persistence(entity) == BLOCKCHAIN:
                bc_entities.append(entity)
                bc_data.append(self._get_bc_data(entity))

        return DataTempManager.select_with_conditionals_in_rdb(
            self.request,
            self.request.q_query,
            bc_data,
            bc_entities
        )

    def _insert_blockchain(self):
        """
        Executes INSERTs into blockchain
        :return: Number of affected rows
        """
        data_transaction = SQLAnalyzer(self.request.q_query).insert_to_dict(
            self.request.q_entities[0]
        )

        return self._persist_data_bc(data_transaction, self.request.q_entities[0])

    def _update_blockchain(self):
        """
        Executes an UPDATE query for blockchain assets.

        1. find assets with name X fulfilling Y conditional in the blockchain (Z = SELECT * FROM X WHERE Y ...)
        2. get the bc index of every entry in Z
        3. delete rows from the index table of X which correspond to each index from Z

        :return: Number of deleted rows
        """
        config = Configuration.get_instance()

        # Get all bc assets for the given entity
        all_entity_data = self._get_bc_data(*self.request.q_entities)

        if all_entity_data is None or len(all_entity_data) == 0:
            return 0

        # Generate a SELECT query using whatever conditionals or operands were given on the original UPDATE query
        query_select = SQLAnalyzer(
            self.request.q_query).generate_select_from_identifier(self.request.q_entities[0], select=DEFAULT_HASH, bring_remainder=False)

        conditional = SQLAnalyzer(self.request.q_query).get_conditonal()
        if conditional:
            query_select += ' ' + conditional

        # Generate updated assets to later append into the blockchain
        updated_asset_tuples = DataTempManager.update_bc_in_rdb(
            self.request,
            self.request.q_query,
            query_select,
            all_entity_data,
            self.request.q_entities[0]
        )

        # print(truncate(updated_asset_tuples))

        if updated_asset_tuples is None or len(updated_asset_tuples) == 0:
            return 0

        index_entries_to_delete = []
        new_assets_to_append = []

        asset_attributes = Mapper.get_columns_from_assets(
            self.request.q_entities[0], all_entity_data).column_names()

        # print(truncate(asset_attributes))

        asset_attributes.insert(0, DEFAULT_ENTITY)
        asset_attributes.append(DEFAULT_PREVIOUS)

        # create the updated assets
        for asset in updated_asset_tuples:
            # get the asset data
            asset = list(asset)
            asset.insert(0, self.request.q_entities[0])
            asset_data = dict(zip(asset_attributes, asset))

            # Add the newly-created asset to a list so we can append them to the blockchain later
            new_assets_to_append.append(asset_data)

            # Add the id of the old asset to a list so we can delete them all later
            index_entries_to_delete.append(asset_data['_previous'])

        # print(truncate(new_assets_to_append))

        # Save the new assets
        start = timer()
        status = self._persist_data_bc(
            new_assets_to_append,
            self.request.q_entities[0]
        )
        end = timer()
        print('>> finished saving new assets in',
              s_to_ms(end-start, 5), 'ms')

        # if any new assets were added, remove index entries from the older ones
        if status > 0:
            start = timer()

            sql_delete_index_entries = Mapper.sql_delete_index_entries(
                self.request.q_entities[0],
                index_entries_to_delete
            )

            request_delete_index = Request(
                q_query=sql_delete_index_entries,
                db_user=config.db_user,
                db_name=config.bc_index_dbname,
                db_password=config.db_password,
                db_port=config.db_port,
                bc_port=config.bc_port,
                db_host=config.db_host,
                bc_host=config.bc_host,
                bc_public_key=config.bc_public_key,
                bc_private_key=config.bc_private_key,
                q_type=DELETE,
                q_entities=[self.request.q_entities[0] + '_index']
            )

            client_sql = ClientSQL(request_delete_index)
            client_sql.start()
            client_sql.join()
            result = client_sql.get_result()

            end = timer()
            print('>> finished deleting old assets in',
                  s_to_ms(end-start, 5), 'ms')

            if result == status:
                return status
        return 0

    def _delete_blockchain(self):
        """
        Executes a DELETE query for blockchain assets.

        1. find assets with name X fulfilling Y conditional in the blockchain (Z = SELECT * FROM X WHERE Y ...)
        2. get the bc index of every entry in Z
        3. delete rows from the index table of X which correspond to each index from Z

        :return: Number of deleted rows
        """
        config = Configuration.get_instance()

        # 1. find assets with name X fulfilling Y conditional in the blockchain (Z = SELECT * FROM X WHERE Y ...)
        bc_data = []
        bc_entities = []
        for entity in self.request.q_entities:
            if SchemaManager.get_entity_persistence(entity) == BLOCKCHAIN:
                bc_entities.append(entity)
                bc_data.append(self._get_bc_data(entity))

        # 2. get the bc index of every entry in Z
        query = SQLAnalyzer(self.request.q_query).generate_select_from_identifier(
            self.request.q_entities[0], str(DEFAULT_HASH))

        bc_hash_list = DataTempManager.select_with_conditionals_in_rdb(
            self.request,
            query,
            bc_data,
            bc_entities,
            True
        )

        if not bc_hash_list:
            return 0

        bc_hash_list = np.array(bc_hash_list)[:, 0].tolist()

        # 3. delete rows from the index table of X which correspond to each index from Z
        sql_delete_index_entries = Mapper.sql_delete_index_entries(
            self.request.q_entities[0],
            bc_hash_list
        )
        request_delete_index = Request(
            q_query=sql_delete_index_entries,
            db_user=config.db_user,
            db_name=config.bc_index_dbname,
            db_password=config.db_password,
            db_port=config.db_port,
            bc_port=config.bc_port,
            db_host=config.db_host,
            bc_host=config.bc_host,
            bc_public_key=config.bc_public_key,
            bc_private_key=config.bc_private_key,
            q_type=DELETE,
            q_entities=[self.request.q_entities[0] + '_index']
        )

        client_sql = ClientSQL(request_delete_index)
        client_sql.start()
        client_sql.join()
        return client_sql.get_result()

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
        # if self.request.q_type == SELECT and
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
            if isinstance(data[key], datetime.datetime) or isinstance(data[key], datetime.date):
                data[key] = str(data[key])

        return data

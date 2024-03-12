import sys
import copy
import typing
from helpers.strings import truncate
import pydash
from logger import log
from threading import Thread
from mapper.column import DEFAULT_ENTITY, DEFAULT_HASH, DEFAULT_PREVIOUS, DEFAULT_STATUS
from mapper.mapper import Mapper
from configuration.config import Configuration
from helpers.dicts import normalize
from sql_analyzer.sql_analyzer import SQLAnalyzer
from sqlclient.clientsql import ClientSQL
from communication.request import Request
from mapper.schema_manager import SchemaManager
from mapper.persistence_model import BLOCKCHAIN
from index_manager.index_manager import IndexManager
from sqlclient.data_temp_manager import DataTempManager
from communication.query_type import DELETE, INSERT, SELECT, UPDATE
from timeit import default_timer as timer
from helpers.floats import s_to_ms
import numpy as np
import datetime
import os
from dotenv import load_dotenv, find_dotenv
from hfc.fabric import Client
import uuid
import asyncio
from pathlib import Path
import os
import json

load_dotenv(find_dotenv())

class ClientBlockchain(Thread):
    network_profile_path = os.getenv('NETWORK_PROFILE', 'test/fixtures/network.json')

    def __init__(self, request):
        super().__init__()
        self.result = None
        self.request = request

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.cli = Client(net_profile=Path(self.network_profile_path))
        self.requester = self.cli.get_user(org_name='org1.example.com', name='Admin')
        self.peers = ['peer0.org1.example.com', 'peer1.org1.example.com']
        self.channel = 'businesschannel'
        self.cc_name = 'moon_cc'

    def run(self):
        if SELECT == self.request.q_type:
            print('starting SELECT...')
            start = timer()
            self.result = self._select_blockchain()
            end = timer()
            print(f"({s_to_ms(end-start, 5)} ms) finished SELECT")

        elif INSERT == self.request.q_type:
            print('starting INSERT...')
            start = timer()
            self.result = self._insert_blockchain()
            end = timer()
            print(f"({s_to_ms(end-start, 5)} ms) finished INSERT")

        elif UPDATE == self.request.q_type:
            print('starting UPDATE...')
            start = timer()
            self.result = self._update_blockchain()
            end = timer()
            print(f"({s_to_ms(end-start, 5)} ms) finished UPDATE")

        elif DELETE == self.request.q_type:
            print('starting DELETE...')
            start = timer()
            self.result = self._delete_blockchain()
            end = timer()
            print(f"({s_to_ms(end-start, 5)} ms) finished DELETE")
        else:
            raise Exception('Unrecognized Request Type')

    def _write_to_bc(self, data: list[dict], entity: str) -> int:
        """
        Stores data on Blockchain and its index on the index table
        :param data: List of dicts with data to be stored
        :param entity: Entity name
        :return: Number of affected rows
        """
        config = Configuration.get_instance()

        try:
            start = timer()
            for item in data:
                asset_id = str(uuid.uuid4())
                tx = self.loop.run_until_complete(self.cli.chaincode_invoke(
                    requestor=self.requester,
                    channel_name=self.channel,
                    peers=self.peers,
                    args=['set', asset_id, json.dumps(normalize(item))],
                    cc_name=self.cc_name,
                    wait_for_event=True
                ))
                print('wrote tx:', tx)

                IndexManager.store_index(
                    entity,
                    item[SchemaManager.get_primary_key_by_entity(entity)],
                    asset_id
                )
                print('wrote index:', asset_id)
            end = timer()
            print(f"({s_to_ms(end-start, 5)} ms) finished writing to blockchain")
        except Exception:
            log.e(
                'Blockchain Client Module',
                sys.exc_info()
            )
            return 0
        return len(data)

    def _get_bc_data(self, *entities) -> list[dict]:
        """
        Returns a formatted list of blockchain transaction data for the given list of entities.

        :param entities: Entities to get blockchain data for.
        :return: A list of dicts of blockchain tx data.
        """
        config = Configuration.get_instance()

        start = timer()

        # start = timer()
        # get a list of lists of blockchain asset ids, separated by asset type
        entities_asset_ids = [IndexManager.get_ids_by_entity(
            self.request, entity) for entity in entities]
        # end = timer()
        # print('finished getting asset ids in', s_to_ms(end-start, 5), 'ms')

        asset_ids = pydash.flatten(entities_asset_ids)

        # TODO: batch-loading assets
        # TODO: get using range, list or specific asset ids
        # TODO: get using timestamps (before, after, between timestamps)
        # start = timer()
        assets = json.loads(self.loop.run_until_complete(self.cli.chaincode_invoke(
            requestor=self.requester,
            channel_name=self.channel,
            peers=self.peers,
            args=['getList', *asset_ids],
            cc_name=self.cc_name,
            wait_for_event=True
        )))

        end = timer()
        print(f"({s_to_ms(end-start, 5)} ms) retrieved assets:", assets)

        return assets

    def _select_blockchain(self):
        """
        Executes a SELECT query for blockchain assets.
        Creates a temp table using the requested entities, executes the SELECT there and returns results.

        :return: List of tuples containing the query results.
        """
        bc_data = []
        bc_entities = []

        for entity in self.request.q_entities:
            if SchemaManager.get_entity_db(entity) == BLOCKCHAIN:
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

        return self._write_to_bc(data_transaction, self.request.q_entities[0])

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

        asset_attributes = Mapper.get_entity_columns(
            self.request.q_entities[0]).column_names()

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
        status = self._write_to_bc(
            new_assets_to_append,
            self.request.q_entities[0]
        )
        end = timer()
        print(f"({s_to_ms(end-start, 5)} ms) finished writing new assets")

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
                db_host=config.db_host,
                q_type=DELETE,
                q_entities=[self.request.q_entities[0] + '_index']
            )

            client_sql = ClientSQL(request_delete_index)
            client_sql.start()
            client_sql.join()
            result = client_sql.get_result()

            end = timer()
            print(f"({s_to_ms(end-start, 5)} ms) finished deleting index entries")

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

        # find assets with name X fulfilling Y conditional in the blockchain (Z = SELECT * FROM X WHERE Y ...)
        bc_data = []
        bc_entities = []
        for entity in self.request.q_entities:
            if SchemaManager.get_entity_db(entity) == BLOCKCHAIN:
                bc_entities.append(entity)
                bc_data.append(self._get_bc_data(entity))

        # TODO: create updated assets with _status: DELETED and _previous: _hash

        # get the bc index of every entry in Z
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

        # delete rows from the index table of X which correspond to each index from Z
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
            db_host=config.db_host,
            q_type=DELETE,
            q_entities=[self.request.q_entities[0] + '_index']
        )

        client_sql = ClientSQL(request_delete_index)
        client_sql.start()
        client_sql.join()
        return client_sql.get_result()

    def get_result(self):
        return self.result

import sys
import csv
import copy
import datetime
from itertools import cycle
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.exceptions import NotFoundError
from bigchaindb_driver.crypto import generate_keypair

QUERIES_PATH = 'twitter/queries.txt'

class ClientBC():
    def __init__(self, port, hosts, pub_key, priv_key):
        self.hosts = cycle(hosts)
        self.port = port
        self.pub_key = pub_key
        self.priv_key = priv_key
        self.logfile = input('Log Filename:')
        if(
            self.logfile is None or
            self.logfile == '' or
            '.' in self.logfile
        ):
            self.logfile = 'log_twitter.csv'
        else:
            self.logfile += '.csv'

    def exec(self, query):
        if query is not None or query == '':
            query = query.rstrip()
            query_type = query.split(' ')[0].upper()
            if query_type == 'SELECT':
                if 'LIMIT' not in query:
                    before = datetime.datetime.now()
                    result = self._select_in_list(query)
                    after = datetime.datetime.now()
                else:
                    before = datetime.datetime.now()
                    result = self._select_limit(query)
                    after = datetime.datetime.now()
            elif query_type in ['DELETE', 'UPDATE', 'INSERT']:
                before = datetime.datetime.now()
                result = self._insert(query)
                after = datetime.datetime.now()
            else:
                return

            response_time = self._calculate_response_time(before, after)
            self._log_result(query, result, response_time)

    def _insert(self, query):
        json_data = self._insert_to_json(query)
        try:
            bdb = BigchainDB(
                'http://{}:{}'.format(next(self.hosts), self.port)
            )
            tx = bdb.transactions.prepare(
                operation='CREATE',
                signers=self.pub_key,
                asset={'data': json_data}
            )
            signed_tx = bdb.transactions.fulfill(
                tx,
                private_keys=self.priv_key
            )
            bdb.transactions.send_sync(signed_tx)
        except Exception:
            print('tried:', json_data)
            print(sys.exc_info())
            return 0
        return 1

    def _select_in_list(self, query):
        entity_data = self._select_data_by_entity('user_profiles')
        response = []
        ids = starts_in = query.find('(') + 1
        ids = query[starts_in:-1]
        ids = ids.replace('\'', '').replace(' ', '').split(',')
        list_in = list(map(int, ids))
        for data in entity_data:
            if (
                'uid' in data and
                'name' in data and
                int(data['uid']) in list_in
            ):
                response.append(tuple([int(data['uid']), data['name']]))
        return response

    def _select_limit(self, query):
        uid = query.split('uid =')[1].split(' ')[1].replace('\'','')
        entity_data = self._select_data_by_entity_limit('tweets', 10, uid)
        response = []
        for data in entity_data:
            response.append(tuple([
                int(data['id'].replace('\'','')),
                int(data['uid'].replace('\'','')),
                data['text'],
                data['createdate'] 
            ]))
        return response

    def _select_data_by_entity(self, entity):
        '''
        return a list of dict data for an entity
        '''
        bdb = BigchainDB(
            'http://{}:{}'.format(next(self.hosts), self.port)
        )
        height = 0
        all_data = []
        try:
            while True:
                block = bdb.blocks.retrieve(block_height=str(height))
                transactions = block['transactions']
                if transactions != []:
                    for item in transactions:
                        if (
                            'entity' in item['asset']['data'] and
                            item['asset']['data']['entity'] == entity
                        ):
                            all_data.append(item['asset']['data'])
                height += 1
        except NotFoundError:
            pass
        finally:
            return all_data

    def _select_data_by_entity_limit(self, entity, quantity, uid):
        '''
        return a list of dict data for an entity
        '''
        bdb = BigchainDB(
            'http://{}:{}'.format(next(self.hosts), self.port)
        )
        height = 0
        all_data = []
        count = 0
        try:
            while count != quantity:
                block = bdb.blocks.retrieve(block_height=str(height))
                transactions = block['transactions']
                if transactions != []:
                    for item in transactions:
                        if (
                            'entity' in item['asset']['data'] and
                            item['asset']['data']['entity'] == entity and
                            item['asset']['data']['uid'] == uid
                        ):
                            all_data.append(item['asset']['data'])
                            count += 1
                            if count == quantity:
                                break
                height += 1
        except NotFoundError:
            pass
        finally:
            return all_data

    def _insert_to_json(self, query):
        '''
        convert string insert sql to JSON
        '''
        entity_name = query.split('INSERT INTO')[1].split(' ')[1]
        lists = query.split('INSERT INTO {} '.format(entity_name))[1].split(' VALUES ')
        cols = lists[0][1:-1].replace('\'','').split(',')
        data = lists[1][1:-1].replace('\'','').split(',')
        for i in range(len(cols)):
            cols[i] = cols[i].strip()
            data[i] = data[i].strip()
        cols.insert(0, 'entity')
        data.insert(0, entity_name)
        dictionary_data = dict(zip(cols, data))
        return dictionary_data

    def _log_result(self, query, result, response_time):
        with open('logs/{}'.format(self.logfile), 'a', newline='') as log_file:
            writer = csv.writer(log_file)
            writer.writerow([query, result, response_time])

    def _calculate_response_time(self, before, after):
        response_time = after-before
        rt_millis = response_time.days * 86400000
        rt_millis += response_time.seconds * 1000
        rt_millis += round(response_time.microseconds/float(1000), 1)
        return rt_millis


if __name__ == "__main__":
    keypair = generate_keypair()
    pub_key = keypair.public_key
    priv_key = keypair.private_key
    cli = ClientBC(
        hosts=['localhost'],
        port=9984,
        pub_key=pub_key,
        priv_key=priv_key
    )
    with open(QUERIES_PATH, 'r', newline='') as queries_file:
        for line in queries_file:
            cli.exec(line)

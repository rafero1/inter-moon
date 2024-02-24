from moon_client.request_model import REQUEST_MODEL
from itertools import cycle
from copy import deepcopy
import json


class RequestGenerator():
    hosts = None
    configs = None

    @staticmethod
    def generate_request(query):
        """
        Generates a new request
        :param query: An SQL Query
        :return: A request on text format
        """
        if RequestGenerator.configs is None:
            with open('config/config.json') as file_config:
                RequestGenerator.configs = json.load(file_config)

        if RequestGenerator.hosts is None:
            RequestGenerator.hosts = cycle(
                RequestGenerator.configs['hosts']
            )
        this_host = next(RequestGenerator.hosts)
        request = deepcopy(REQUEST_MODEL)
        request['q_query'] = query
        request['db_user'] = RequestGenerator.configs['db_user']
        request['db_name'] = RequestGenerator.configs['db_name']
        request['db_password'] = RequestGenerator.configs['db_password']
        request['db_port'] = RequestGenerator.configs['db_port']
        request['bc_port'] = RequestGenerator.configs['bc_port']
        request['db_host'] = this_host
        request['bc_host'] = this_host
        request['bc_public_key'] = RequestGenerator.configs['bc_public_key']
        request['bc_private_key'] = RequestGenerator.configs['bc_private_key']

        return json.dumps(request)

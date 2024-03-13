from hfc.fabric import Client
from asyncio import AbstractEventLoop, get_event_loop
from pathlib import Path
import os
import uuid
import json

root = Path(__file__).parent

loop = get_event_loop()

cli = Client(net_profile=root.joinpath('test/fixtures/network.json'))

# User instance with the Org1 admin's certs
org1_admin = cli.get_user(org_name='org1.example.com', name='Admin')
# User instance with the Org2 admin's certs
org2_admin = cli.get_user(org_name='org2.example.com', name='Admin')
# User instance with the orderer's certs
orderer_admin = cli.get_user(org_name='orderer.example.com', name='Admin')

# gopath_bak = os.environ.get('GOPATH', '')
# gopath = os.path.normpath(os.path.join(
#     root,
#     'test/fixtures/chaincode'
# ))
# os.environ['GOPATH'] = os.path.abspath(gopath)
os.environ['GOPATH'] = root.joinpath('moon').as_posix()


def setup_org():
    response = loop.run_until_complete(cli.channel_create(
        orderer='orderer.example.com',
        channel_name='businesschannel',
        requestor=org1_admin,
        config_yaml='test/fixtures/e2e_cli/',
        channel_profile='TwoOrgsChannel'
    ))
    print("create_channel:\n", response)

    response = loop.run_until_complete(cli.channel_join(
        requestor=org1_admin,
        channel_name='businesschannel',
        peers=['peer0.org1.example.com',
               'peer1.org1.example.com'],
        orderer='orderer.example.com'
    ))
    print("join:\n", response)

    response = loop.run_until_complete(cli.channel_join(
        requestor=org2_admin,
        channel_name='businesschannel',
        peers=['peer0.org2.example.com',
               'peer1.org2.example.com'],
        orderer='orderer.example.com'
    ))
    print("join:\n", response)


def install_cc():
    cli.new_channel('businesschannel')

    return loop.run_until_complete(cli.chaincode_install(
        requestor=org1_admin,
        peers=['peer0.org1.example.com',
               'peer1.org1.example.com'],
        cc_path='github.com/rafero1',
        cc_name='moon_cc',
        cc_version='v1.0'
    ))


def init_cc():
    return loop.run_until_complete(cli.chaincode_instantiate(
        requestor=org1_admin,
        channel_name='businesschannel',
        peers=['peer0.org1.example.com'],
        args=[],
        cc_name='moon_cc',
        cc_version='v1.0'
    ))


def invoke_cc(args):
    cli.new_channel('businesschannel')

    response = loop.run_until_complete(cli.chaincode_invoke(
        requestor=org1_admin,
        channel_name='businesschannel',
        peers=['peer0.org1.example.com'],
        args=args,
        cc_name='moon_cc',
        transient_map=None,  # optional, for private data
        # for being sure chaincode invocation has been commited in the ledger, default is on tx event
        wait_for_event=True,
        # cc_pattern='^invoked*' # if you want to wait for chaincode event and you have a `stub.SetEvent("invoked", value)` in your chaincode
    ))
    return json.loads(response)


if __name__ == "__main__":
    # setup_org()

    # response = install_cc()
    # print("install:\n", response)

    # response = init_cc()
    # print("init:\n", response)

    # response = invoke_cc(['getList', 'd643ba95-5063-493d-9b6f-1020bcb2136e', '081c4c9f-3991-4083-afdd-e8d0c83181ec'])
    # print("invoke:\n", json.loads(response))

    # response = invoke_cc(['get', '6ab8f728-28c6-458f-a2ae-2ced71d88be5'])
    # print("invoke:\n", json.loads(response))

    key = str(uuid.uuid4())
    asset = json.dumps({"__entity": "user_files", "name": "Marcos", "email": "marco@example.com",
                       "result": True, "value": 0.5, "unit": "mg/dL", "date": "2020-01-01"})
    print(key, asset)
    response = invoke_cc(['set', key, asset])
    print("invoke:\n", response)

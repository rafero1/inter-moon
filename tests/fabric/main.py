from hfc.fabric import Client
from asyncio import AbstractEventLoop, get_event_loop
from pathlib import Path
import os

root = Path(__file__).parent

if __name__ == "__main__":
    loop = get_event_loop()

    cli = Client(net_profile=root.joinpath('network.json'))

    # print(cli.organizations)  # orgs in the network
    # print(cli.peers)  # peers in the network
    # print(cli.orderers)  # orderers in the network
    # print(cli.CAs)  # ca nodes in the network

    org1_admin = cli.get_user(org_name='org1.example.com', name='Admin') # User instance with the Org1 admin's certs
    org2_admin = cli.get_user(org_name='org2.example.com', name='Admin') # User instance with the Org2 admin's certs
    orderer_admin = cli.get_user(org_name='orderer.example.com', name='Admin') # User instance with the orderer's certs



    # response = loop.run_until_complete(cli.channel_create(
    #     orderer='orderer.example.com',
    #     channel_name='businesschannel',
    #     requestor=org1_admin,
    #     config_yaml='test/fixtures/e2e_cli/',
    #     channel_profile='TwoOrgsChannel'
    # ))

    # responses = loop.run_until_complete(cli.channel_join(
    #     requestor=org1_admin,
    #     channel_name='businesschannel',
    #     peers=['peer0.org1.example.com',
    #     'peer1.org1.example.com'],
    #     orderer='orderer.example.com'
    # ))

    # responses = loop.run_until_complete(cli.channel_join(
    #     requestor=org2_admin,
    #     channel_name='businesschannel',
    #     peers=['peer0.org2.example.com',
    #     'peer1.org2.example.com'],
    #     orderer='orderer.example.com'
    # ))

    # cli.new_channel('businesschannel')

    responses = loop.run_until_complete(cli.chaincode_install(
        requestor=org1_admin,
        peers=['peer0.org1.example.com',
                'peer1.org1.example.com'],
        cc_path='github.com/example_cc',
        cc_name='example_cc',
        cc_version='v1.0'
    ))

    # Instantiate Chaincode in Channel, the response should be true if succeed
    response = loop.run_until_complete(cli.chaincode_instantiate(
        requestor=org1_admin,
        channel_name='businesschannel',
        peers=['peer0.org1.example.com'],
        args=['a', '200', 'b', '300'],
        cc_name='example_cc',
        cc_version='v1.0'
    ))

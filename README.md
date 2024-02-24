# README

Inter-MOON is a very experimental tool to enable cross-querying functionality between a blockchain network and a relational database using only SQL.

## Requirements

Needs python 3.6.9, bigchaindb 2.2.2 and tendermint 0.31.5.

Developed using Ubuntu 18.04 LTS.

Needs the following packages: gcc-10, g++-10, jq, monit, python3-pip, python3-dev, python3-tk, tk-dev, zlib1g-dev, libbz2-dev, libpq-dev, libssl-dev, libffi-dev, liblzma-dev, libsqlite3-dev, libreadline-dev, build-essential, among other standard python3 development packages.

Requirements in `requirements.txt` must be installed in order to run any of the modules in this repo.

Execute pip commands with [CC=gcc-10 or clang](https://github.com/pyenv/pyenv-virtualenv/issues/410#issuecomment-1221123961) if using pyenv on Ubuntu 22.

## moon

Contains the main Inter-MOON client, functions and toolset.

Make and setup the environment config using:

    cp .env.example .env

Configure the blockchain asset structure definition by editing the `mapper/catalog.json` file.

Start the client:

    python3 start_moon.py

## test_cli

Contains scripts for generating test data and sending data to an existing client.

Make and setup the environment config using:

    cp .env.example .env
    cp config/config.json.example config/config.json

The script will read and prompt the user to execute any query files inside `queries`. There's also some basic custom query functionality.

Run these to generate new files, if desired, or use them as basis to make new build scripts:

    python3 build_bdb.py
    python3 build_rdb.py

Start the client api script:

    python3 start_cli_moon_2.py

## bigchaindb help

The [Official BigchainDB documentation](https://docs.bigchaindb.com/projects/server/en/latest/) is a good place to start.
[Set up node](https://docs.bigchaindb.com/en/latest/installation/node-setup/set-up-node-software.html)
[Set up a network](https://docs.bigchaindb.com/en/latest/installation/network-setup/network-setup.html)

## Quick node setup

Install required libs and packages:

    sudo apt-get update
    sudo apt-get install -y python3-pip python3-dev libssl-dev libffi-dev build-essential
    sudo pip3 install -U pip
    sudo pip3 install -U setuptools
    sudo pip3 install bigchaindb==2.2.2

Run below with default values, except for API Server bind, which should be set to `0.0.0.0:9984`:

    bigchaindb configure

Install tendermint:

    sudo apt install -y unzip
    wget https://github.com/tendermint/tendermint/releases/download/v0.31.5/tendermint_v0.31.5_linux_amd64.zip
    unzip tendermint_v0.31.5_linux_amd64.zip
    rm tendermint_v0.31.5_linux_amd64.zip
    sudo mv tendermint /usr/local/bin

    tendermint init

Install monit:

    sudo apt install -y monit

    bigchaindb-monit-config

    monit -d 1

## Scripts

Helper scripts can be found in the `scripts` folder. They most likely will have to be edited to work with your environment.

`bdb-reset-all.sh` resets bigchaindb, tendermint and clears logs.
`bdb-check-peers.sh` checks the status of the tendermint peers.
`bdb-copy-env.sh` can copy the scripts, genesis.json and config.toml example files to the specified nodes. Use it like this: `./bdb-copy-env.sh "user" "password"`

## General network setup

1. Setup each node in the network individually. Make sure they are all working properly before trying to make a network. Test them by starting bigchaindb with `bigchaindb start`, check the logs and run some tests. `curl http://0.0.0.0:9984` should return something.
1. Choose a node to be the first node in the network. This node will be the one that will create the genesis block (Coordinator).
1. Once they are all working, get the node id, public key, address, and hostname of each node. You can get the node id with `tendermint show_node_id`. The public key and address can be found in the `.tendermint/config/priv_validator_key.json` file. The hostname is the node's IP address.
1. Setup the Coordinator's `genesis.json` file with the correct public key and address of each node (including itself) in the validators section. As for the node names, they can be anything you want.
1. Copy the modified `genesis.json` to each node (replace the older one). Every node needs to have the same genesis.json file.
1. Setup the Coordinator's `config.toml`. For simplicity's sake, use the included config.toml.example as base. Make sure to:
   - Set the moniker name to something unique.
   - Set the persistent_peers to the node id and hostname of each node (including itself).
1. Copy the modified `config.toml` to each node (replace the older one). Just make sure each node has a unique moniker.
1. Start the Coordinator node. It will create the genesis block and start the network.
1. Start the other nodes. They will connect to the network and start syncing.
1. Check the status of the network using the `bdb-check-peers.sh` script. If your nodes are connected, you should see some information about the other peers.

## Problem solving

Setting up nodes and a network (even a local one) can be tricky. Many problems can occur depending on the environment.

- Make sure you have the correct versions of the software installed. Every node needs to be running the same version of BigchainDB and Tendermint. The versions used in this project are BigchainDB 2.2.2 and Tendermint 0.31.5. Same goes for python. Use python 3.6.9 for safety.
- Make sure you have the correct ports open. Every node needs to be able to communicate with the other nodes. The ports used in this project are 9984, 9985, 46656 and 46657. Or you can disable iptables.
- Every node needs to have the same genesis.json. Same goes for the config.toml file (except for the moniker name, which must be unique for each node).
- Setup each node individually first before setting up the network. This will help you identify problems.
- Start the network with a 2-node setup. Once you have that working, you can add more nodes.
- Before changing configuration files, make sure you stop the node using monit `monit stop all`.
- If tendermint complains about DB errors, try dropping the bigchaindb database using `bigchaindb drop`. Naturally, this will erase all the data in the database.
- If tendermint complains about other node's addresses or chain ID, make sure you have the same genesis.json files in each node.
- If bigchaindb complains about "chain start time", make sure your genesis.json file is correctly setup and drop the bigchain database before trying again.
- Sometimes, bigchaindb gives errors but doesn't log them properly. If there seems to be an error and you can't find it in the logs, try running the node without monit `bigchaindb start`.
- If bigchaindb complains about greenlet, make sure you're using python 3.6.9 and try installing greenlet 0.4.16 and gevent 20.6.0.
- If you can't identify the problem, you can use `bdb-reset-all.sh` to reset bigchaindb and tendermint and start over.

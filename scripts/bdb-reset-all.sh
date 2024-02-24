#!/bin/bash

# reset bigchaindb and tendermint
monit stop all
monit stop all
yes | bigchaindb drop
tendermint unsafe_reset_all
rm -fr /home/rafael/.tendermint
tendermint init

# clear logs
sh /home/rafael/bdb-clear-logs.sh

# show new node id, address & public key
tendermint show_node_id
cat /home/rafael/.tendermint/config/priv_validator_key.json

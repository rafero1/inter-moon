#!bin/bash

monit stop all
yes | bigchaindb drop
tendermint unsafe_reset_all
rm -rf ~/.tendermint
tendermint init
bigchaindb init
monit start all
sleep 2 && monit summary

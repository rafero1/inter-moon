#!/bin/bash

curl -s localhost:26657/net_info | jq ".result.peers[].node_info | {id, listen_addr, moniker}"

#!/bin/bash

USER=$1
PASSWORD=$2
SERVERS=("10.102.20.108" "10.102.20.194" "10.102.21.75" "10.102.21.47" "10.102.21.45")

cp /home/rafael/genesis.json.example /home/rafael/.tendermint/config/genesis.json

for i in "${!SERVERS[@]}"; do
  SERVER=${SERVERS[$i]}
  MONIKER="bc-$((i+1))"

  sshpass -p "$PASSWORD" scp "/home/$USER/bdb-*.sh" "$USER@$SERVER:/home/$USER/"
  sshpass -p "$PASSWORD" scp "/home/$USER/genesis.json.example" "$USER@$SERVER:/home/$USER/.tendermint/config/genesis.json"
  sed "s/_moniker_/$MONIKER/" "/home/$USER/config.toml.example" | sshpass -p "$PASSWORD" ssh "$USER@$SERVER" "cat > /home/$USER/.tendermint/config/config.toml"
done

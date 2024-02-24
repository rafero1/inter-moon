## Checklist
### CLIENT
- Check queries file
- Check database name
- Check address to send requests

### SERVER
- Check the catalog file

### DATABASE
- Execute the ddl file on database CLI
- Execute any additional data insertion sql, if necessary

### BLOCKCHAIN
- Reset tendermint
- Drop bigchain

### Commands

### Reset bigchaindb and tendermint
``` bash
monit stop all
yes | bigchaindb drop
tendermint unsafe_reset_all
rm -rf ~/.tendermint
tendermint init
bigchaindb init
monit start all
```

### Reset logs
``` bash
rm -rf ~/bigchaindb.logs ~/bigchaindb-error.logs ~/.bigchaindb-monit/logs
```

### Quickly execute a sql script
``` bash
psql -v ON_ERROR_STOP=1 -1 -h <hostname> -f ddl-reset-lab-results.sql index_bc
```

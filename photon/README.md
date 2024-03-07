# Photon CLI

Run Photon locally. To run it just run:
```
cargo run
``` 
And Photon will index all accounts/transactions in localnet. Before responding to any API calls, it will always
update it's indexing to include all prior transactions/accounts. It only works against a localnet 
validator because it's impractical to index all devnet/mainnet historical transactions/accounts locally.  

You can further specify Solana test validator port (defaults to 8889) and the local port for Photon 
api (defaults to 9001):
```
cargo run --validator-port 8889 --api-port 9001 
```

Test Photon indexer does not preserve state between runs so to clear data you can simply restart it. 

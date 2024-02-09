# Photon

Lightweight indexer for general & token compression. WIP.

## Database

Photon uses Postgres and the schema is managed by SeaORM. The database is managed via migrations. 
For MacOS users, we recommend using Homebrew to run local Postgres: https://wiki.postgresql.org/wiki/Homebrew.

Run the following command install the database:
```
export DATABASE_URL="postgres://postgres@localhost/postgres"
cargo run -p migration -- up
```

## Local Testing

Photon supports Localnet. It uses a block-based poller instead of gRPC due to issues with running Geyser plugins on MacOS.

1. Setup the Light Protocol repository.
```
cd .. & git clone https://github.com/Lightprotocol/light-protocol.git
```

2. Build the Light contracts locally.
TBD. Note that in the future we can pull these from devnet or mainnet.

3. Deploy the local validator.
```
solana-test-validator --reset --limit-ledger-size=500000000 \
    --bpf-program DmtCHY9V1vqkYfQ5xYESzvGoMGhePHLja9GQ994GKTTc ../light-protocol/cli/bin/account_compression.so \
    --bpf-program 9sixVEthz2kMSKfeApZXHwuboT6DZuT6crAYJTciUCqE ../light-protocol/cli/bin/psp_compressed_token.so \
    --bpf-program noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV ../light-protocol/cli/bin/spl_noop.so \
    --account-dir ../light-protocol/cli/accounts
```

4. Use their CLI to mint example tokens.
TBD.
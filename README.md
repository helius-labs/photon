# Photon

Lightweight indexer for general & token compression. WIP.

## Running Photon 

To run the photon indexer run:

```
cd photon

// By default photon runs against localnet
cargo run

// Against devnet
cargo run -- --rpc-url=https://api.devnet.solana.com

// Using your local Postgres database instead of the default in in-memory SQLite db
cargon run -- --db-urlpostgres://postgres@localhost/postgres
```

To see configurations options run
```
cd photon 
cargo run --help
```

## Database

Photon uses Postgres and the schema is managed by SeaORM. The database is managed via migrations. 
For MacOS users, we recommend using Homebrew to run local Postgres: https://wiki.postgresql.org/wiki/Homebrew.

Install sea-orm-cli:
```
cargo install sea-orm-cli --version 0.10.6
```

Run the following command to install the database:
```
export DATABASE_URL="postgres://postgres@localhost/postgres"
cargo run --bin migration -- up
```

Run the following command to generate the DB models.
```
sea-orm-cli generate entity -o src/dao/generated
```

## Integration Tests
First, setup your local database following the steps above. Then run the following:
```
cargo test -- --nocapture
```
The `nocatpure` argument is optional. It will show you the logs when this flag is included.

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

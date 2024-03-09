# Photon

Solana indexer for general & token compression

## Running Photon 

To run the photon indexer run:

```bash
# Against localnet
cargo run

# Against devnet
cargo run -- --rpc-url=https://api.devnet.solana.com

# Using your local Postgres database instead of the default in in-memory SQLite db
cargon run -- --db-urlpostgres://postgres@localhost/postgres

# Specifying a start slot. Defaults to 0 for localnet and current for devnet/mainnet
cargo run -- --start-slot=123 

# To see more configuration options
cargo run -- --help
```

## Local Development

### Running Tests

To run tests, install and run Postgres and SQLlite locally. For MacOS users, we recommend using
Homebrew to run local Postgres: https://wiki.postgresql.org/wiki/Homebrew.

Then export environment variables to configure your RPC:

```bash
export MAINNET_RPC_URL=https://api.devnet.solana.com
export DEVNET_RPC_URL=https://api.mainnet-beta.solana.com
```

Afterwards, run:
```bash
cargo test
```

### Database Management

We support both Postgres and SQLite through sea-orm. 

To run migrations run:
```bash
export DATABASE_URL="postgres://postgres@localhost/postgres" # Or your SQLlite database url
cargo run --bin migration -- up
```

To generate database models first install sea-orm-cli:
```bash
cargo install sea-orm-cli --version 0.10.6
```

Then run:
```bash
sea-orm-cli generate entity -o src/dao/generated
```

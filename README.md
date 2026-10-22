# Photon: the Indexer for ZK Compression on Solana

Photon is the core indexer for [ZK Compression](https://www.zkcompression.com) on the Solana blockchain. It offers rapid indexing capabilities, snapshot support, and flexible database options to cater to local and production deployments.

## 🚀 Quick Start

### Installation

1. Install dependencies:

```bash
sudo apt install -y build-essential pkg-config libssl-dev
```

2. Install `photon-indexer`:

```bash
cargo install photon-indexer
```

### 🔧 Usage

#### Basic Usage 

* Run Photon with default settings against localnet:

```bash
photon
```

#### Configuration

* Connect to Devnet:

```bash
photon --rpc-url=https://api.devnet.solana.com
```

* Use gRPC for block streaming (requires GRPC_X_TOKEN env variable):

```bash
photon --rpc-url=https://api.devnet.solana.com --grpc-url=<grpc_url>
```

* Use a local Postgres database:

```bash
photon --db-url=postgres://postgres@localhost/postgres
```

* Specify a start slot:

```bash
photon --start-slot=123
```

* For more advanced options:

```bash
photon --help
```

#### Ingestion recovery

Configure fallback RPC endpoints with access to the same cluster and the history
you need to index:

```bash
photon --db-url=postgres://postgres@localhost/photon \
  --rpc-url=https://primary.example.com \
  --rpc-fallback-url=https://archive.example.com
```

`--rpc-fallback-url` accepts repeated flags or comma-separated URLs. Without it,
the native Solana RPC client is used directly. Optional failover for `get*` calls delegates HTTP,
JSON-RPC parsing, timeouts and HTTP 429 retries to that same SDK.

RPC polling enumerates produced slots and fetches ordered windows bounded by
`--max-concurrent-block-fetches`, repairing omitted predecessors through parent
links. Memory depends on that configured window and decoded block sizes; later
windows are not fetched while the current window waits on a missing predecessor.

Transient failures back off with jitter up to 30 seconds. All referenced trees,
including V2 queues and events without outputs, must be resolved or confirmed
excluded before the batch commits. Indexing restarts from the database checkpoint
after a panic or unexpected stream termination. RPC request timeouts and the
five-second gRPC-to-RPC fallback handle source outages. Quiet chains can wait;
valid batches and SDK retries are not cancelled by a blanket progress timeout. Replayed
blocks must match committed hashes. Errors keep the checkpoint in place and
`getIndexerHealth` reports failed workers or unresolved work. Deterministic
indexing failures are rechecked every 30 seconds. Metadata is refreshed each minute.

Parent or committed-block hash conflicts require canonical-chain reconciliation
or restoring a consistent snapshot. The main ingestor detects these conflicts
and stops advancing; it does not automatically roll back indexed state.

Run the ingestion fault-injection tests without external services:

```bash
cargo test --lib
cargo test --test integration_tests ingester_robustness
```

## 📸 Snapshots

Photon supports snapshots for quick bootstrapping. 

### Loading a Snapshot

1. Download a snapshot:

```bash
photon-snapshot-loader --snapshot-dir=~/snapshot --snapshot-server-url=https://photon-devnet-snapshot.helius-rpc.com
```

2. Run Photon with the snapshot:

```bash
photon --snapshot-dir=~/snapshot --rpc-url=https://api.devnet.solana.com --db-url=postgres://postgres@localhost/postgres
```

### Creating Snapshots

Create a local snapshot:
```bash
photon-snapshotter --snapshot-dir=~/snapshot
```

Store snapshots in an R2 bucket:
```bash
photon-snapshotter --r2-bucket=some-bucket --r2-prefix=prefix
```

Note: Set `R2_ACCESS_KEY`, `R2_ACCOUNT_ID`, and `R2_SECRET_KEY` environment variables when using R2.

## 🗄️ Database Management

Photon supports both Postgres and SQLite. By default, it uses an in-memory SQLite database.

To use a custom database:
```bash
export DATABASE_URL="postgres://postgres@localhost/postgres"
# Set ENABLE_CUSTOM_INDEXES=true to enable program-specific indexes. 
photon-migration up
photon --db-url=$DATABASE_URL
```

## 🗄️ Custom Indexes

Developers can easily add program-specific indexes through a custom migration to speed up queries. See `src/migration/migrations/custom/custom20252201_000001_init.rs` for an example. In the future, we will add tooling to make it easier to add custom indexes. For now, contact the Helius team to add custom indexes.

## 🛠️ Local Development

### Running Tests

1. Set up the environment:
```bash
export MAINNET_RPC_URL=https://api.mainnet-beta.solana.com
export DEVNET_RPC_URL=https://api.devnet.solana.com
export TEST_DATABASE_URL="postgres://postgres@localhost/postgres"
```

2. Install additional tools:
```bash
npm install -g @apidevtools/swagger-cli
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
docker run -p 3001:3001 docker.io/pmantica1/light-prover:1
```

3. Run tests:
```bash
cargo test
```

Note: All migrations run automatically during tests for both Postgres and SQLite.

### Database Model Generation

```bash
cargo install sea-orm-cli --version 0.10.6
sea-orm-cli generate entity -o src/dao/generated
```

### API Documentation

Generate OpenAPI schemas:
```bash
cargo run --bin=photon-openapi
```

## 📬 Support

For support or queries, please open an issue on Github or contact the [Helius discord](https://discord.gg/HjummjUXgq).

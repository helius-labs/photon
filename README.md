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

Apply PostgreSQL migrations before the tests, as in CI. SQLite migrations run inside the test fixtures.

### API profiling

The optional `api-profiling` feature records handler duration, success/error status,
and database statement counts and duration from the existing test workloads. It is
disabled in normal builds. The additional fixture workloads cover every registered
RPC implementation, including repeatable offline responses for Solana RPC calls.

Start a local PostgreSQL database and prover, configure the test environment above,
and apply pending PostgreSQL migrations:

```bash
DATABASE_URL="$TEST_DATABASE_URL" cargo run --bin photon-migration -- up
```

Capture a profile using a fresh output path for each run:

```bash
PHOTON_API_PROFILE_OUT=/tmp/photon-before.jsonl \
PHOTON_PROFILE_REPETITIONS=20 \
cargo test --release --features api-profiling --test integration_tests -- \
  --test-threads=1 --skip merkle_tree_deserialization:: --skip open_api_tests::
```

The skipped groups require live Devnet access or OpenAPI tooling and add no handler
profiling coverage. Profile files append to the selected path. Handler spans do not
include JSON-RPC transport or response serialization; use complete HTTP requests
when measuring those costs.

Compare matching workloads after another build:

```bash
python3 scripts/profile_api.py \
  --baseline /tmp/photon-before.jsonl --candidate /tmp/photon-after.jsonl \
  --json-output /tmp/photon-profile.json --markdown-output /tmp/photon-profile.md
```

The report separates PostgreSQL and SQLite, errors and successful calls, and
unmatched or undersampled test groups. A faster deep page does not establish the
same improvement for a first page or point lookup.

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

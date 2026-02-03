# Snowflake-to-FalkorDB Migration Tool

Rust-based CLI to migrate and continuously sync structured data from Snowflake into a FalkorDB graph, using a declarative mapping.

## Features

- Live Snowflake integration via `snowflake-connector-rs`
- JSON/YAML config describing mappings from tables/columns to nodes and edges
- Batched, parameterized Cypher `UNWIND` + `MERGE` into FalkorDB
- Incremental sync using an `updated_at` watermark per mapping
- Delete semantics via `deleted_flag` columns
- Purge options (entire graph or selected mappings)
- Daemon mode for periodic syncs
- Global and per-mapping metrics via HTTP endpoint

## Installation

Prerequisites:

- Rust toolchain (stable)
- Snowflake account with a user/warehouse/role that can read the relevant tables
- FalkorDB instance reachable from where you run the tool

Clone the repo and build:

```bash
cargo build --release
```

The binary will be at `target/release/snowflake_to_falkordb`.

## Configuration

Config is JSON or YAML, auto-detected by file extension.

### Top-level structure

```yaml
snowflake:
  account: "MY_ACCOUNT"
  user: "LOAD_USER"
  # one of the following auth methods:
  password: "********"             # password auth
  # private_key_path: "/path/to/key.pem"  # keypair auth (PEM)
  warehouse: "WH"
  database: "DB"
  schema: "PUBLIC"
  role: "SYSADMIN"
  query_timeout_ms: 60000

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "customer_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"                  # or "none" / "falkordb" (file is implemented)
  file_path: "state.json"          # optional, default: state.json

mappings:
  - type: node
    name: customers
    source:
      table: "DB.PUBLIC.CUSTOMERS" # or "select: ..." for custom SQL
      where: "ACTIVE = TRUE"       # optional extra predicate
    mode: incremental               # or "full"
    delta:
      updated_at_column: "UPDATED_AT"
      deleted_flag_column: "IS_DELETED"
      deleted_flag_value: true
      initial_full_load: true       # full once, then incremental
    labels: ["Customer"]
    key:
      column: "CUSTOMER_ID"
      property: "customer_id"
    properties:
      email:   { column: "EMAIL" }
      country: { column: "COUNTRY" }

  - type: node
    name: orders
    source:
      table: "DB.PUBLIC.ORDERS"
    mode: incremental
    delta:
      updated_at_column: "UPDATED_AT"
    labels: ["Order"]
    key:
      column: "ORDER_ID"
      property: "order_id"
    properties:
      amount: { column: "AMOUNT" }

  - type: edge
    name: customer_orders
    source:
      table: "DB.PUBLIC.ORDERS"
    mode: incremental
    delta:
      updated_at_column: "UPDATED_AT"
    relationship: "PURCHASED"
    direction: out                   # from customer to order
    from:
      node_mapping: customers
      match_on:
        - column: "CUSTOMER_ID"
          property: "customer_id"
    to:
      node_mapping: orders
      match_on:
        - column: "ORDER_ID"
          property: "order_id"
    key:
      column: "ORDER_ID"            # optional unique edge id
      property: "order_id"
    properties: {}
```

Key points:

- `source.table` + optional `source.where` are used to generate SELECT statements.
- If `delta.updated_at_column` is set and a watermark exists, the tool adds:
  - `AND updated_at_column > '<last_watermark>'` to the query.
- If `source.select` is used, the query is taken as-is (you manage watermark predicates manually).

Watermarks per mapping are stored in the `state` backend (currently `file`), keyed by mapping name.

## Running the tool

### Single run

```bash
cargo run --release -- \
  --config path/to/config.yaml
```

This will:

1. Connect to Snowflake and FalkorDB.
2. For each mapping:
   - Load rows from Snowflake (full or incremental).
   - Transform to nodes/edges.
   - Write in batches via `UNWIND` + `MERGE`.
   - Apply deletes if `deleted_flag_column`/`deleted_flag_value` are configured.
   - Update watermarks.

### Purge modes

#### Purge entire graph

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --purge-graph
```

This runs `MATCH (n) DETACH DELETE n` before loading.

#### Purge specific mappings

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --purge-mapping customers \
  --purge-mapping customer_orders
```

- Node mapping purge removes all nodes with the mapping's labels.
- Edge mapping purge removes all relationships of that mapping's relationship type between the associated labels.

### Daemon mode (periodic sync)

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --daemon \
  --interval-secs 300
```

Behavior:

- Runs an initial sync (optionally with purge flags) and then repeats every `interval-secs` seconds.
- On subsequent runs, purge flags are ignored; only incremental syncs run.
- Errors per run are logged via `tracing` and counted in metrics.

## Authentication to Snowflake

Two modes are supported via `SnowflakeConfig`:

1. **Password auth**

   ```yaml
   snowflake:
     account: "MY_ACCOUNT"
     user: "LOAD_USER"
     password: "********"
     # ...
   ```

2. **Keypair auth** (encrypted PEM)

   ```yaml
   snowflake:
     account: "MY_ACCOUNT"
     user: "LOAD_USER"
     private_key_path: "/path/to/key.pem"  # PEM file
     password: "passphrase"                # optional PEM passphrase
     # ...
   ```

If `private_key_path` is set, the tool uses keypair auth; otherwise it falls back to password auth. One of `password` or `private_key_path` must be set.

## Metrics and Monitoring

A lightweight HTTP metrics server is started automatically on:

- `0.0.0.0:9898`

Fetch metrics:

```bash
curl http://localhost:9898/
```

Example output (Prometheus-style):

```text
snowflake_to_falkordb_runs 3
snowflake_to_falkordb_failed_runs 0
snowflake_to_falkordb_rows_fetched 12345
snowflake_to_falkordb_rows_written 12000
snowflake_to_falkordb_rows_deleted 345
snowflake_to_falkordb_mapping_runs{mapping="customers"} 3
snowflake_to_falkordb_mapping_failed_runs{mapping="customers"} 0
snowflake_to_falkordb_mapping_rows_fetched{mapping="customers"} 8000
snowflake_to_falkordb_mapping_rows_written{mapping="customers"} 7800
snowflake_to_falkordb_mapping_rows_deleted{mapping="customers"} 200
snowflake_to_falkordb_mapping_runs{mapping="orders"} 3
...
```

These metrics let you see, per mapping, how many rows were fetched, written, and deleted, and how often each mapping ran.

## Operational notes

- **Idempotency**: node/edge writes use `MERGE` on configured keys, so re-running the same data is safe.
- **Incremental safety**: watermarks are only advanced after successful writes; if a run fails mid-way, the next run will retry from the last successful watermark.
- **Deletes**: any row where `deleted_flag_column == deleted_flag_value` is treated as deleted:
  - Node mappings: matching nodes are `DETACH DELETE`d.
  - Edge mappings: matching relationships are `DELETE`d.
- **Ordering**: mappings are processed in the order listed; for edges, the referenced node mappings must exist in the config.
- **Logging**: uses `tracing` with log level controlled by `RUST_LOG`, e.g. `RUST_LOG=info`.

## Troubleshooting

- Check metrics at `http://localhost:9898/` to see if a particular mapping is stuck (e.g. zero rows written or growing failed runs).
- Enable debug logging:

```bash
RUST_LOG=debug,snowflake_to_falkordb=debug cargo run --release -- --config cfg.yaml
```

- Verify Snowflake connectivity and credentials if you see errors during `fetch_rows_for_mapping`.
- Verify FalkorDB endpoint and graph name if you see Cypher execution errors.

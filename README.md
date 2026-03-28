# Scala + Sail POC

Proof of concept: Scala Spark Connect client talking to [Sail](https://github.com/lakehq/sail) (DataFusion backend).

## Architecture

```
Scala (Spark Connect Client) --> gRPC/Protobuf --> Sail Server --> DataFusion (Rust)
```

## Prerequisites

- JDK 17+
- sbt
- Docker (for running Sail)

## Quick Start

### 1. Start Sail server

```bash
docker compose up -d
```

### 2. Run the examples

```bash
# Basic connection test (range, SQL, DataFrame ops)
sbt "runMain com.poc.sail.BasicConnection"

# SQL-heavy example (GROUP BY, window functions, CTEs)
sbt "runMain com.poc.sail.SqlExample"

# CSV read -> transform -> Parquet write
sbt "runMain com.poc.sail.CsvExample"
```

### 3. Custom host/port

```bash
SAIL_HOST=myserver SAIL_PORT=50051 sbt "runMain com.poc.sail.BasicConnection"
```

## What each example does

| Example | Description |
|---|---|
| `BasicConnection` | Connects, creates DataFrames in-memory, groupBy + filter |
| `SqlExample` | Temp views + pure SQL: aggregations, window functions, CTEs |
| `CsvExample` | Reads `data/sample.csv`, transforms, writes Parquet |

## Notes

- Sail implements the Spark Connect protocol, so the Scala code is **standard Spark API** - no vendor lock-in.
- The only difference vs real Spark is `.remote("sc://host:port")` instead of `.master("local[*]")`.
- Not all Spark features are supported by Sail yet. Check [Sail compatibility](https://github.com/lakehq/sail) for details.

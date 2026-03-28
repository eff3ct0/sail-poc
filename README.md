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

```bash
# 1. Start Sail server
docker compose up -d

# 2. Run any example
sbt "runMain com.poc.sail.BasicConnection"
sbt "runMain com.poc.sail.basic.E01_ReadFormats"
sbt "runMain com.poc.sail.extreme.E16_MegaQuery"

# 3. Custom host/port
SAIL_HOST=myserver SAIL_PORT=50051 sbt "runMain com.poc.sail.BasicConnection"
```

## Examples Catalog

### Basic (`com.poc.sail.basic`)

| # | Class | What it tests |
|---|-------|---------------|
| E01 | `E01_ReadFormats` | Read CSV, JSON (nested), Parquet; write Parquet and read back |
| E02 | `E02_WriteFormats` | Write CSV (custom delimiter), JSON, Parquet (snappy), partitioned writes |
| E03 | `E03_SelectFilterSort` | Select, filter, sort, limit, distinct, drop, rename, withColumn |
| E04 | `E04_BasicAggregations` | count, sum, avg, min, max, stddev, groupBy single/multiple columns |

### Intermediate (`com.poc.sail.intermediate`)

| # | Class | What it tests |
|---|-------|---------------|
| E05 | `E05_AllJoinTypes` | inner, left, right, full outer, cross, left semi, left anti joins |
| E06 | `E06_WindowFunctions` | rank, dense_rank, row_number, lag, lead, ntile, cumulative sum/avg, percent_rank, cume_dist, first/last_value |
| E07 | `E07_PivotUnpivot` | Pivot (groupBy + pivot + agg), unpivot via stack(), monthly pivot |
| E08 | `E08_SetOperations` | union, union distinct, unionByName, intersect, except, exceptAll |

### Advanced (`com.poc.sail.advanced`)

| # | Class | What it tests |
|---|-------|---------------|
| E09 | `E09_NestedTypes` | Struct access, explode, posexplode, array functions (size, contains, sort, distinct), maps, flatten, arrays_zip |
| E10 | `E10_HigherOrderFunctions` | transform, filter, aggregate, exists, forall on arrays; chained HOFs |
| E11 | `E11_ComplexSQL` | Correlated subqueries, EXISTS/NOT EXISTS, IN subquery, scalar subqueries, HAVING, nested CTEs, multi-level CTE chains |
| E12 | `E12_DateTimeFunctions` | Date extraction (year/month/day/dow/quarter), arithmetic (add/sub/months_between), formatting, timestamp ops, time windows, date-based windowing |

### Extreme (`com.poc.sail.extreme`)

| # | Class | What it tests |
|---|-------|---------------|
| E13 | `E13_RecursiveCTE` | `WITH RECURSIVE`: org hierarchy traversal, Fibonacci generation, date series |
| E14 | `E14_WindowAcrobatics` | Sessionization (gap detection), Gaps & Islands problem, multiple overlapping windows (SMA-3, SMA-5, Bollinger bands, Williams %R) |
| E15 | `E15_ComplexTypeMadness` | Deeply nested structs (3+ levels), array of structs explode+re-aggregate, map_from_entries, collect_list of collect_list, nested schemas |
| E16 | `E16_MegaQuery` | Single query: 6 CTEs + window functions + correlated logic + CASE + date functions + multiple joins + aggregations |
| E17 | `E17_UDFAndCatalog` | Scala UDF registration via Spark Connect, SHOW DATABASES/TABLES, DESCRIBE, DROP VIEW, dynamic column ops |
| E18 | `E18_StringAndRegex` | String functions (upper/lower/trim/reverse/initcap), substring/position, regexp_extract, regexp_replace, LIKE/RLIKE, get_json_object, concat_ws+collect_list |
| E19 | `E19_EdgeCases` | Null handling (coalesce/nanvl/nullif/nvl), null-safe equality (<=>), empty DataFrames, type coercion, special char column names, 20+ chained transformations, large CASE/IN |
| E20 | `E20_MultiJoinAnalytics` | Self-joins, inequality joins with NOT EXISTS, peer cohort comparison, 6-table join with composite scoring via multiple PERCENT_RANK windows |

### Original examples (`com.poc.sail`)

| Class | What it tests |
|-------|---------------|
| `BasicConnection` | Connection test, range, SQL, groupBy, filter |
| `SqlExample` | Temp views, GROUP BY, window rank, CTE |
| `CsvExample` | CSV read, transform, Parquet write |

## What to expect

Some examples will likely fail on Sail, and that's the point. Track which ones succeed and which don't to map Sail's current compatibility surface. Known areas that may have gaps:

- Recursive CTEs (E13)
- Scala UDFs over Spark Connect (E17)
- Some higher-order array functions (E10)
- Complex type operations like `map_from_entries` (E15)
- `window()` time-bucketing function (E12)
- `queryExecution` introspection (E19)

## Notes

- All examples use `SailSession()` which reads `SAIL_HOST`/`SAIL_PORT` env vars (defaults: `localhost:50051`)
- The code is **standard Spark API** - no vendor lock-in
- The only difference vs real Spark is `.remote("sc://host:port")` instead of `.master("local[*]")`

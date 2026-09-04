# Why Columnar Storage

## The I/O Problem

A table with 20 columns and 1 billion rows. You query 3 columns.

**Row-oriented (PostgreSQL)**:
- Row size: 20 columns × ~50 bytes/column = ~1000 bytes/row
- To read 3 columns: read all 1000 bytes per row
- Total I/O: 1 billion × 1000 bytes = **1 TB**
- Useful data: 3/20 columns = **150 GB**
- I/O waste: ~85%

**Column-oriented (ClickHouse)**:
- Read only the 3 queried columns
- Total I/O: ~150 GB of raw data → compressed further
- With zstd compression on integer columns: ~30-60 GB
- I/O waste: ~0%

For analytical queries that touch 2–5 of 20+ columns, columnar storage provides a 5–50× I/O reduction — which directly translates to query speed.

---

## Compression Amplification

Columnar storage also compresses dramatically better than row storage.

A column of 1 billion `country` values has ~200 distinct values. Run-length encoding or dictionary encoding reduces this to near-zero bytes per row for sorted data.

A row store mixes `country` with `user_id` (high cardinality), `timestamp` (always increasing), and `latency_ms` (random integers). The mixture compresses poorly.

Modern OLAP codecs:
- **LZ4**: fast, moderate compression
- **ZSTD**: slower, better compression (2–10× over LZ4)
- **Delta encoding**: efficient for timestamps and monotonic IDs
- **Dictionary encoding**: efficient for low-cardinality strings

ClickHouse typically achieves 10–30× compression on analytics data compared to raw JSON.

---

## Vectorised Execution

Modern CPUs have SIMD (Single Instruction Multiple Data) instructions that operate on arrays of 4–16 values simultaneously.

Traditional (row-at-a-time):
```
for each row:
    if row.country == 'india':
        sum += row.latency_ms
```

Vectorised:
```
# Process 16 rows at once using AVX-512 instructions
countries_batch = load_16_countries()
mask = countries_batch == 'india'
latencies_batch = load_16_latencies()
sum += sum_masked(latencies_batch, mask)
```

ClickHouse processes data in granules (8192 rows by default) using vectorised operations throughout. This is fundamentally faster than row-at-a-time processing.

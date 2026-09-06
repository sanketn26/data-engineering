---
title: Object Storage Internals
description: A data lake is not a distributed filesystem with prettier URLs — object semantics explain why table formats exist.
---

# Object Storage Internals

**Time:** 35 minutes reading + 30 minutes exercise<br>
**Prerequisites:** [Data Movement](data-movement.md)<br>
**Outcomes:** name which S3 operations are expensive and why; explain why rename-heavy systems (Hive-style "overwrite a partition") struggle on object storage; connect object-store limitations to why Iceberg maintains manifests instead of listing files.

[Data Movement](data-movement.md) covers the *cost* of reading from S3 (bandwidth, cross-AZ, locality). This page covers the *semantics* — what S3 actually promises you, which is less than a filesystem, in ways that shape every table format and lakehouse decision in this academy.

**A data lake is not a distributed filesystem with prettier URLs.** HDFS gave you a real hierarchical namespace, atomic rename, and (usually) strong read-after-write consistency, because it was designed to *be* a filesystem. S3 and its equivalents (GCS, Azure Blob) are key-value stores with a flat namespace that merely *looks* hierarchical because keys contain `/`.

## Object vs file semantics

| Filesystem (HDFS, local disk) | Object storage (S3) |
|---|---|
| Real directories; `ls` is cheap and consistent | No real directories — `s3://bucket/a/b/c` is one key; "listing a prefix" scans key ranges |
| Atomic rename (metadata pointer swap) | No atomic rename — a "rename" is copy + delete, non-atomic, cost and time scale with object size |
| Append to a file | Objects are immutable once written — no in-place append; a rename or update means writing a new object |
| Strong consistency historically assumed | Modern S3 is strongly read-after-write consistent, but many object stores and older configurations are not — never assume without checking your specific store |

## What each operation actually costs

| Operation | What it does | Cost characteristic |
|-----------|--------------|----------------------|
| `PUT` | Write a whole object | Priced per request; large objects should use multipart |
| `GET` | Read a byte range | Cheap per request, but many small `GET`s (one per tiny file) dominate cost and latency versus fewer large ones |
| `HEAD` | Fetch metadata only | Cheap, but still a full request round trip — don't `HEAD` before every `GET` "to check it exists" in a hot loop |
| `LIST` | Enumerate keys under a prefix | **Expensive at scale** — paginated, rate-limited, and O(number of keys), not O(1) like a directory listing |
| Multipart upload | Split a large `PUT` into parts, uploaded in parallel, then assembled | Required in practice above a few hundred MB; failed parts must be cleaned up or they silently accrue storage cost |
| Range read | `GET` with a byte offset/length | The mechanism that makes Parquet footer reads and row-group pruning possible — see [why columnar storage](../olap/columnar-storage.md) |

!!! production-gotcha "LIST is the operation that breaks at scale, not GET"
    A job that lists `s3://events/date=2024-01-15/` to discover files before reading them is fine at 200 files and a slow-motion incident at 2 million files. `LIST` calls are paginated (typically 1,000 keys per call) and each page is a network round trip against a service with its own request-rate limits. This is the concrete mechanism behind "too many small files" — it is not just about compute reading tiny objects, it is about the **planning phase** timing out before a single byte of data is read.

## Consistency and request-rate considerations

Object stores handle high aggregate request volume by internally partitioning key ranges, historically keyed by prefix. A common practice — request-rate hotspotting on a shared prefix like `s3://bucket/2024-01-15/` when thousands of writers hit the same date partition simultaneously — is why some architectures randomize a prefix component (a hash prefix before the date) purely to spread request load, even though it makes the "directory" unintuitive to browse by hand. Check your specific object store's current guidance before doing this — the need for it has decreased over time as providers have improved automatic partitioning, but "just add a hash prefix" persists as folklore in some codebases long after it stopped being necessary, so verify rather than cargo-cult it.

## Why rename-heavy systems struggle

Classic Hive-style "overwrite a partition" is:

```text
1. Write new files to a temporary location.
2. Delete the old partition's files.
3. Rename (move) the new files into place.
```

On HDFS, step 3 is a fast, atomic metadata operation. On S3, "rename" does not exist as a primitive — it is implemented as copy-then-delete, which is neither atomic nor cheap, and can leave the table in a half-moved state if the job dies mid-operation. A reader that lists the directory during that window sees an inconsistent set of files: some old, some new, possibly some missing.

This single semantic gap — no atomic rename, no atomic multi-file commit — is the concrete reason table formats exist. [Why Table Formats Exist](../lakehouse/why-table-formats.md) covers the "where is the table?" question in full; this page is the storage-layer reason that question has no good answer on raw object storage.

## Why Iceberg uses manifests instead of `LIST`

Ask this question out loud in a design review — it is one of the fastest ways to check whether someone actually understands table formats:

> Why does Iceberg maintain a tree of manifest files instead of asking S3 to `LIST` a million Parquet files?

Because `LIST` at that scale is slow, rate-limited, and gives you no way to atomically know "these are the files that made up the table at time T." A manifest list (pointing to manifests, pointing to data files, with per-file stats) is:

- **A single small set of reads** instead of a paginated scan of the whole key space — planning a query becomes reading a few KB of metadata, not listing millions of keys.
- **Atomic** — a new snapshot is a new metadata pointer; readers either see the whole old snapshot's file set or the whole new one, never a half-committed mix.
- **Prunable** — manifest-level min/max stats skip whole manifests (and their files) before a single data file is opened, which `LIST` cannot do because it has no concept of file contents.

This is also why [manifests are also central to Delta's transaction log](../lakehouse/delta.md) and Hudi's timeline — every modern table format solves the same object-storage semantic gap with the same shape of answer: an authoritative, atomically-updated metadata layer that replaces `LIST` and `rename` as the source of truth.

## How it fails { #failure-modes }

- A job lists a prefix with millions of small files during planning and times out or hits request-rate throttling before scanning any data.
- A crashed job during a copy-then-delete "overwrite" leaves a partition with a mix of old and new files, silently double-counting or dropping rows.
- Hot-prefix throttling on a shared, non-randomized key pattern under many concurrent writers (verify current guidance for your object store before "fixing" this pre-emptively).
- Treating eventual consistency assumptions from an old blog post as still true — check your specific provider's current consistency model rather than folklore.
- Millions of orphaned multipart upload parts from failed uploads that were never aborted, accruing storage cost invisibly.

## Check your understanding { #exercise }

A nightly Spark job writes `s3://analytics/events/date=2024-01-15/` by writing 8,000 small files (one per task, unpartitioned further), then a separate step lists that prefix to hand the file list to a downstream Trino query.

1. Name the two distinct costs this pattern incurs on object storage (one at write time, one at read/planning time).
2. The job is later moved to write into an Iceberg table instead of raw Parquet. What replaces the "list the prefix" step, and why is it both faster and safer against a job crashing mid-write?
3. The team wants to "overwrite" `date=2024-01-15` in the raw-Parquet version if the job needs to rerun. What can go wrong if the job crashes halfway through the delete+rewrite?

??? success "Exit check"
    (1) Write-time: 8,000 `PUT`s instead of, say, 20 well-sized ones — more request overhead and a small-file problem for every future reader. Read-time: `LIST`ing 8,000+ keys is paginated and rate-limited, and query planning waits on it before the query itself starts. (2) Iceberg's manifest list replaces the `LIST` call — Trino reads a small, versioned set of manifest files that already name every data file and its stats, atomically reflecting one snapshot, so it never sees a partial write. (3) A crash mid-delete-and-rewrite (no atomic rename on S3) can leave a mix of old and new files, or delete the old files before all new ones landed — a reader querying that partition during the window gets wrong or missing rows with no error raised.

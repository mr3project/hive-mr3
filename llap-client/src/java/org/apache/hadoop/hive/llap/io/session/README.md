# LLAP IO Session Caching

This package provides a minimal per-(DAG, table) on-heap cache used by MR3
workers. Sessions are opened via `LlapIo.openSession` which allocates isolated
caches for data and metadata along with an `InflightTracker` to deduplicate
concurrent loads. Each reader receives a `CacheContext` and all cache operations
are routed through this handle.

Configuration keys:
- `mr3.llap.session.data.cache.bytes`
- `mr3.llap.session.metadata.cache.bytes`
- `mr3.llap.session.admission.watermark`

Workers should open a session when the first task for a table arrives and close
it when all tasks for that table complete. Closing the session drops all cached
content.

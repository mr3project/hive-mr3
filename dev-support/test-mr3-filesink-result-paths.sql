-- End-to-end HiveServer2/Beeline smoke test for the four MR3 FileSink result
-- paths affected by hive.server2.thrift.resultset.serialize.in.tasks.
--
-- Run this file through Beeline against HiveServer2.  Running it through the
-- standalone CLI does not exercise the HiveServer2-only result contracts.
-- Example:
--   beeline -u 'jdbc:hive2://localhost:10000/default' \
--     -f dev-support/test-mr3-filesink-result-paths.sql
--
-- A successful run prints the ordinary result rows for 1A and 1B and column
-- statistics for 2A and 2B.  It also fails at the relevant statement if any
-- result cannot be transported, materialized, fetched, or decoded.

SET hive.execution.engine=mr3;
SET hive.stats.autogather=false;
-- ORC asks Hadoop for configured key providers while opening a writer.  This
-- test does not use encrypted ORC data or HDFS encryption zones, so prevent a
-- stale KMS URI inherited from core-site.xml/hdfs-site.xml from instantiating
-- KMSClientProvider on MR3 workers.
SET hadoop.security.key.provider.path=;
SET dfs.encryption.key.provider.uri=;

DROP TABLE IF EXISTS mr3_filesink_stats_2a;
DROP TABLE IF EXISTS mr3_filesink_stats_2b;

CREATE TABLE mr3_filesink_stats_2a (
  id INT,
  label STRING
) STORED AS ORC;

CREATE TABLE mr3_filesink_stats_2b (
  id INT,
  label STRING
) STORED AS ORC;

INSERT INTO mr3_filesink_stats_2a VALUES
  (1, 'alpha'),
  (2, 'beta'),
  (2, 'beta'),
  (NULL, NULL);

INSERT INTO mr3_filesink_stats_2b VALUES
  (10, 'ten'),
  (20, 'twenty'),
  (20, 'twenty'),
  (NULL, NULL);

-- 1A: ordinary user result, task-side Thrift serialization disabled.
-- Expected ordered rows:
--   1A  1  alpha
--   1A  2  beta
--   1A  2  beta
--   1A  NULL  NULL
SET hive.server2.thrift.resultset.serialize.in.tasks=false;
SELECT '1A' AS scenario, id, label
FROM mr3_filesink_stats_2a
ORDER BY id NULLS LAST, label NULLS LAST;

-- 2A: internal column-statistics output with task-side Thrift serialization
-- disabled.  On commit bf680978 this takes the direct in-memory text path.
-- DESCRIBE must report id low/high values 1/2 and one null, and label must
-- report one null.  Successful ANALYZE also proves StatsTask decoded the
-- internal output and persisted it to the metastore.
ANALYZE TABLE mr3_filesink_stats_2a COMPUTE STATISTICS FOR COLUMNS id, label;
DESCRIBE FORMATTED mr3_filesink_stats_2a id;
DESCRIBE FORMATTED mr3_filesink_stats_2a label;

-- 1B: ordinary user result, task-side Thrift serialization enabled.
-- The rows travel as Thrift batches in DAG events and are reconstructed into
-- a SequenceFile before HiveServer2 fetches them.  Expected ordered rows:
--   1B  10  ten
--   1B  20  twenty
--   1B  20  twenty
--   1B  NULL  NULL
SET hive.server2.thrift.resultset.serialize.in.tasks=true;
SELECT '1B' AS scenario, id, label
FROM mr3_filesink_stats_2b
ORDER BY id NULLS LAST, label NULLS LAST;

-- 2B: internal column-statistics output while task-side Thrift serialization
-- is enabled for user results.  Statistics are not JDBC results, so they keep
-- the direct in-memory text contract used by 2A.
-- DESCRIBE must report id low/high values 10/20 and one null, and label must
-- report one null.
ANALYZE TABLE mr3_filesink_stats_2b COMPUTE STATISTICS FOR COLUMNS id, label;
DESCRIBE FORMATTED mr3_filesink_stats_2b id;
DESCRIBE FORMATTED mr3_filesink_stats_2b label;

-- Leave the session at its default setting and remove all test objects.
SET hive.server2.thrift.resultset.serialize.in.tasks=false;
DROP TABLE mr3_filesink_stats_2a;
DROP TABLE mr3_filesink_stats_2b;

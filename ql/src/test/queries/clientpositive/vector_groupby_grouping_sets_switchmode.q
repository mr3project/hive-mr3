set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.groupby.mapaggr.checkinterval=100;
CREATE TABLE ss_hive_count_fail(reference1 string, reference2 string, reference3 int) STORED AS orc;
CREATE TABLE ss_hive_count_fail_tmp(reference1 string, reference2 string, reference3 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/vector_groupingsets_switchmode.csv' OVERWRITE INTO TABLE ss_hive_count_fail_tmp;
INSERT INTO TABLE ss_hive_count_fail SELECT * from ss_hive_count_fail_tmp;
select reference1, reference2, count (reference3) from (select * from ss_hive_count_fail order by reference1 limit 40) as tt group by reference1, reference2 GROUPING SETS((reference1,reference2),(reference1),(reference2),());
set hive.vectorized.execution.enabled=false;
select reference1, reference2, count (reference3) from (select * from ss_hive_count_fail order by reference1 limit 40) as tt group by reference1, reference2 GROUPING SETS((reference1,reference2),(reference1),(reference2),());

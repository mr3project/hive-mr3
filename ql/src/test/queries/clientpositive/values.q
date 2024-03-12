set hive.cli.print.header=true;

explain
VALUES(1,2,3),(4,5,6);
VALUES(1,2,3),(4,5,6);

explain
SELECT * FROM (VALUES(1,2,3),(4,5,6)) as foo;
SELECT * FROM (VALUES(1,2,3),(4,5,6)) as foo;


explain
WITH t1 AS (VALUES('a', 'b'), ('b', 'c'))
SELECT * FROM t1 WHERE col1 = 'a'
UNION ALL
SELECT * from t1 WHERE col1 = 'b';

WITH t1 AS (VALUES('a', 'b'), ('b', 'c'))
SELECT * FROM t1 WHERE col1 = 'a'
UNION ALL
SELECT * from t1 WHERE col1 = 'b';


explain
VALUES(1, 'a', NULL, 10.0)
UNION ALL
VALUES(2, 'b', NULL, 20.0)
UNION ALL
VALUES(3, 'c', NULL, 30.0);

VALUES(1, 'a', NULL, 10.0)
UNION ALL
VALUES(2, 'b', NULL, 20.0)
UNION ALL
VALUES(3, 'c', NULL, 30.0);
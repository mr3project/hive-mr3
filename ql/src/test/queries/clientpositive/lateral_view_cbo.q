
CREATE TABLE simple_table (col string, array_col array<string>);

explain SELECT myTable.myCol FROM simple_table
LATERAL VIEW explode(array(1,2,3)) myTable AS myCol;

explain SELECT myTable.myCol, myTable2.myCol2 FROM simple_table
LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2;

explain SELECT tf.col1, tf.col2, tf.col3
FROM simple_table
  LATERAL TABLE(VALUES('A', 10, simple_table.col),('B', 20, simple_table.col)) AS tf(col1, col2, col3);

explain SELECT myTable.myCol FROM simple_table
LATERAL VIEW explode(simple_table.array_col) myTable AS myCol;

explain SELECT myCol FROM
(SELECT * FROM simple_table
LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2) a WHERE col='0';

explain SELECT myCol FROM simple_table
LATERAL VIEW explode(simple_table.array_col) myTable AS myCol where myCol = 1;


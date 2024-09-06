--! qt:dataset:src
SET hive.insert.into.multilevel.dirs=true;

SET hive.output.file.extension=.txt;

INSERT OVERWRITE DIRECTORY 'target/data/x/y/z/' SELECT src.* FROM src;

dfs -cat ${system:build.dir}/data/x/y/z/*.txt;

dfs -rm -r -f ${system:build.dir}/data/x;

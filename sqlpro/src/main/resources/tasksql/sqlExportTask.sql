-- 当任务类型为数据导入导出时，必须设置的参数
-- use bigdata;
set spark.app.name = 'Spark import-export demo';
set pbear.batch_size = 2000000;

-- HDFS(对外表征为一个表)导出到pg：将HDFS中/hdfs-root-path/my_testdb/my_test_table/导出到pg表user_cube_b
insert overwrite table pbear_market_pg.user_cube_b
select client_no,name,age from my_testdb.my_test_table;

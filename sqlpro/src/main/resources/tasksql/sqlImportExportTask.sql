-- 当任务类型为数据导入导出时，必须设置的参数
-- use bigdata;
set spark.app.name = 'Spark import-export demo';
set pbear.batch_size = 1000000;
-- 注：使用外部数据源均要先使用create命令进行注册。
-- mysql导入到HDFS：将mysql表user_cube数据导入到HDFS /hdfs-root-path/my_testdb/my_test_table/目录中
create temporary table tempTable from pbear_bigdata_mysql with "select client_no,name,age from user_cube";
-- insert xxx select xxx为标准HQL语法
insert overwrite table my_testdb.my_test_table select * from tempTable;

-- HDFS(对外表征为一个表)导出到pg：将HDFS中/hdfs-root-path/my_testdb/my_test_table/导出到pg表user_cube_b
insert overwrite table pbear_market_pg.user_cube_b
select client_no,name,age from my_testdb.my_test_table;

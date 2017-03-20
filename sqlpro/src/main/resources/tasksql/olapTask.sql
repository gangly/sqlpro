-- 运行参数设置
use bigdata;

set pbear.sql/
set spark.app.name = "SQL Service Demo";
set spark.executor.memory = 1G;
-- ElasticSearch相关,暂时只考虑支持：Map和json三种数据写入格式
-- set es.resource = "pbear/docs";
-- 不需要指定集群中所有节点,会自动发现
set es.nodes = "localhost";
set es.port = 9200;

--挂载jar包
add jar /apps/libs/test.jar;

--UDF重命名：用户上传的udf jar包有待测试
--create  TEMPORARY FUNCTION myUdf as 'com.pingan.pbear.common.UdfCollections.myUdf'; 

--注册外部数据源
CREATE TEMPORARY TABLE user_bank_card_0 FROM pbear_market_pg WITH "select client_no,bank_code from o2o_focusbank  where bank_code != 'PAB'";

-- 从hdfs注册临时表。注：with里面的sql语句用到的表名就是临时表名
CREATE TEMPORARY TABLE user_bank_card_1 FROM '/data/train/' format json WITH "select title,summary,myLen(summary) as summaryLen from json file";


-- 中间结果持久化
-- INSERT OVERWRITE TABLE pbear_market_pg.user_bank_card_test select client_no,bank_code,myUdf(bank_code) as code_lowcase from user_bank_card_0;

-- 结果存入外部数据库，使用标准hql语法执行OLAP任务
--INSERT OVERWRITE TABLE pbear_news_mysql.result_table
--select a.client_no,a.bank_code,b.number from user_bank_card_0 as a join user_consumer as b on(a.client_no = b.client_no);

-- 结果存入Hive数据库，使用标准hql语法执行OLAP任务
--INSERT OVERWRITE TABLE bigdata.user_bank_card
--select a.client_no,a.bank_code,b.number from user_bank_card_0 as a join user_consumer as b on(a.client_no = b.client_no);

-- 结果存入HDFS,使用标准hql语法执行OLAP任务 
insert overwrite directory '/data/results/olapdemo/' format json 
select a.client_no,a.bank_code,b.number from user_bank_card_0 as a join user_consumer as b on(a.client_no = b.client_no);

-- 结果存入ES中,需要指定'索引名/类型'以及存入格式,当前只支持json|map两种存入格式
-- insert into es 'pbear/news' format map
-- insert into es 'pbear/news' format json
-- select a.client_no,a.bank_code,b.number from user_bank_card_0 as a join user_consumer as b on(a.client_no = b.client_no);


set spark.app.name = onlinetest;
--set pbear.task.type = batch;

set df.show.rows = 5;

--hive to hive
--CREATE TEMPORARY TABLE pbear_test FROM  hive "" select name, age from test.people2 where age > 7;
--insert into hive "table=test.people3" select name,   age   from pbear_test;
--select * from pbear_test;

-- hdfs to hdfs
--CREATE TEMPORARY TABLE pbear_test FROM  hdfs "path=hdfs://localhost:9000/data/people/|format=json"  select name, age from people where age > 7;
--T1 = select name, age from pbear_test where age > 10;
--insert into hdfs "path=hdfs://localhost:9000/data/people2/|format=json" select name, age from T1;


---- mongo to mongo
--CREATE TEMPORARY TABLE people FrOM  mongo "host=localhost:27017|database=test|collection=people|user=pbear|password=pbear123" select name, age from people where age > 7;
--insert into mongo "host=localhost:27017|database=test|collection=people2" select name, age from people;
--
---- rdb to rdb
--CREATE TEMPORARY TABLE pbear_test FROM  rdb "key=RD_OUEWR_pg" select name, age from people where age > 7;
--insert into rdb  "key=RD_OUEWR_pg|table=people2|mode=overwrite" select name, age from pbear_test;
--select name, age from pbear_test;

-- es to es
--CREATE TEMPORARY TABLE people FROM  es "nodes=localhost|port=9200|index=test/employee|format=json" select name, age from people where age > 7;
--insert into es 'nodes=localhost|port=9200|index=test/employee2|format=json' select name, age from people;
--select name, age from people;

-- kafka to kafka
--set pbear.task.type = streaming;
---- 指定数据源，可以为kafka或者hdfs文件，暂时只支持kafka
--set streaming.source = kafka;
---- 指定流时间间隔,单位秒
--set streaming.interval = 5;
--add jar udfs-1.0-SNAPSHOT.jar;
--CREATE TEMPORARY FUNCTION parser AS 'com.pingan.pbear.udtf.LineParser';
--
--CREATE TEMPORARY TABLE people FROM  kafka "broker=localhost:9092|topic=hello|offset=largest|group=pbear" select parser(line) as (name, age) from stream;
--insert into kafka 'broker=localhost:9092|topic=hello2' select name, StrLen(name) as age from people;
--

-- export test, pg to hive
set pbear.task.type = export;
set pbear.batch_size = 2000000;
CREATE TEMPORARY TABLE pbear_test FROM  rdb "key=RD_OUEWR_pg" select name, age from people;
insert into hive "table=test.people2" select name, age from pbear_test;
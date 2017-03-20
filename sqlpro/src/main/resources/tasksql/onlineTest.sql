set spark.app.name = onlinetest;
set pbear.task.type = batch;



CREATE TEMPORARY TABLE pbear_test FROM  rdb "key=RD_OUEWR_RD" select name, age from people where age > 7;
CREATE TEMPORARY TABLE pbear_test FROM  hdfs "path=/sdf/jisd|format=json"  select name, age from people where age > 7;
CREATE TEMPORARY TABLE pbear_test FROM  hive "" select name, age from people where age > 7;
CREATE TEMPORARY TABLE pbear_test FROM  kafka "broker=localhost:9092|topic=hello|offset=largest|group=pbear" select name, age from people where age > 7;
CREATE TEMPORARY TABLE pbear_test FROM  mongo "host=localhost:27017|database=test|collection=stu" select stu.school.sch_name, age from people where age > 7;
CREATE TEMPORARY TABLE pbear_test FROM  es "nodes=localhost|port=9200|index=pbear/news|format=json" select stu.school.sch_name, age from people where age > 7;

--table1=select name, func(age) as newage from people;
--table2=select name, func(age) as newage from table1;
--
--add file "/Users/lovelife/git/pbear-offline/news/news/src/main/resources-debug/strlen.py"
---- hive to hive
--insert into table test.people3 select name, age from test.people2;
--insert into test.people3 select transform(name, age) using "python strlen.py" as (n, s) from test.people2;

insert into hive "table=people" select name, StrLen(name) as age from test.people2;
insert into hdfs "path=/sdf/|format=json" select name, age from people;
insert into rdb  "key=RD_OOUD_RD|table=people3" select name, age from people;
insert into kafka "broker=localhost:9092|topic=hello" select name, age from people;
insert into mongo "host=localhost:27017|database=test|collection=stu" select name, age from people;
insert into es 'nodes=localhost|port=9200|index=pbear/news|format=json' select name, age from people;

--T1=select a from b;

-- hive to pgs
--INSERT into TABLE PBEAR_test_pg.people
--select ansjWordSegToString(name) as name, age from test.people2;


--
--CREATE TEMPORARY TABLE pbear_test FROM pbear_test_pg.people WITH "select name, age from people where age > 7";
--
--insert overwrite directory '/apps-data/hduser1402/pbear-data/pg' format json
--select a.name,a.age from pbear_test as a join people2 as b on(a.name = b.name);

--
CREATE TEMPORARY TABLE pbear_test FROM pbear_test_pg.people WITH "select name, age from people";
insert into table test.people2
select name, age from pbear_test;

--insert into table test.people3(name, age) select Ip2Country(name), age from test.people2;
--insert into table test.people3(name, age) select Ip2Country('220.181.111.188'), 20;
--insert into table test.people3 select name, age from test.people2;
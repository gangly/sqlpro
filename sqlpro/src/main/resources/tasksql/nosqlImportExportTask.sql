use jk_daikuan_safe;
set spark.app.name = 'Spark import-export demo';
set mongo.input.uri.c1 =mongodb://localhost:27017/test.user;
set mongo.input.split.create_input_splits.c1 = false;
set mongo.input.fields.c1 = {"username":"1","info.school":"1"};

set mongo.input.uri.c2 =mongodb://localhost:27017/test.student;
set mongo.input.split.create_input_splits.c2 = false;
set mongo.input.fields.c2 = {"name":"1","age":"1","info.no":"1","info.class":"1"};



create temporary table user from pbear_cdp_mongo with "select mongo.input.fields.c1 from mongo.input.uri.c1 where mongo.input.query.c1";
insert overwrite table hive_user select * from user;
create temporary table student from pbear_cdp_mongo with "select mongo.input.fields.c2 from mongo.input.uri.c2 where mongo.input.query.c2";
insert overwrite table hive_student select * from student;

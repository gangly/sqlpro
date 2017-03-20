#PBEAR SQL 设计与实现

## 一. 语法设计
注：关键字大小写不敏感，使用大小写均可。
### 0. USE指令
语法：use pbmarket;
功能：指定使用的hive仓库
```
use pbmarket;
```
### 1. SET指令
语法：SET *key* = *value* 
功能：设置应用参数。包括Spark运行参数，Kakfa参数，自定义参数等。
```sql
-- Spark运行参数设置
set spark.app.name = "SQL Service Demo";
set spark.executor.memory = 1G;
set spark.executor.num = 4;

-- kafka参数设置
set kafka.zk_enabled = false;
set kafka.brokers = "10.20.15.219:9092,10.20.15.219:9091";
set kafka.offset = "smallest";
set kafka.topics = "webtrends,news";

-- 自定义参数设置
set pbear.batch_size = 1000000;
```

### 2. ADD指令
语法：ADD [jar|file] *fileName*
功能：加载文件到spark上下文。
```
-- 从HDFS指定路径导入jar到当前上下文
add jar /apps/etl/bi_cube_etl.jar;
-- 从HDFS指定路径导入文件到当前上下文,可以是python，php脚本等
add file /apps/data/parser.py;
```

### 3. CREATE TEMPORARY TABLE指令
 功能：从外部数据源或者HDFS目录注册临时表。
**语法：**
CREATE TEMPORARY TABLE *aaa* FROM *db* WITH *query1*
**或**
CREATE TEMPORARY TABLE *aaa* FROM *path* FORMAT *[json|parquet]* WITH *query2*

**参数说明：**
query1：过滤语句为标准的SQL语法
query2：当数据源为HDFS文件时，需要显示指定文件的格式

*注：**query语句最好使用引号引起来**，默认使用Hive数据库，因此计算业务中需要用到Hive中的数据，不需要显示调用create方法注册数据源。*

**示例1**：将pg数据库pbear_market_pg的表user_profile所有字段注册为临时表uInfo。

```sql
CREATE TEMPORARY TABLE uInfo FROM pbear_market_pg WITH "select * from user_profile"；
```

**示例2**：将pg数据库pbear_market_pg的表user_profile特定字段注册为临时表uInfo。

```sql
CREATE TEMPORARY TABLE uInfo FROM pbear_market_pg WITH "select client_no from user_profile where sex = 0"；
```
    
**示例3**：将HDFS目录/data/users/下文件注册为临时表uInfo，只选择其中的client_no字段。

```sql
CREATE TEMPORARY TABLE uInfo FROM "/data/users/" FORMAT json WITH "select client_no from parquet file"；
```

### 4. CREATE TEMPORARY FUNCTION指令
功能：UDF注册
**语法：**
CREATE TEMPORARY FUNCTION *funcName* AS *packageFullName*
**或者**
TRANSFORM(xx,xx,...) USING [script]
**说明：**UDF可以以jar包，python/php脚本等形式提供。

**示例：**使用jar包提供的UDF
```sql
-- add jar /apps/etl/bi_cube_etl.jar;
CREATE TEMPORARY FUNCTION myLen AS "com.pingan.pbear.myLen";
-- SELECT a, myLen(b) FROM T1;
```

### 5. TRANSFORM(xxx) USING *script*指令
功能：在4的基础上，提供脚本语言支持。
**语法：**
CREATE TEMPORARY FUNCTION *funcName* AS *packageFullName*
**方式2：**使用脚本程序提供的UDF
```sql
-- add file /apps/data/parser.py;
SELECT TRANSFORM(userid,rating) USING 'python parser.py' FROM user_profile;
```

### 6. INSERT/SELECT指令
**语法：** 使用标准HQL语法。暂时支持如下两种语法，后续可以考虑增加更复杂的语法支持。
INSERT [OVERWRITE|INTO] TABLE *targetTable* SELECT *XXX*;
INSERT [OVERWRITE|INTO]  DIRECTORY *targetPath*   **FORMAT [JSON|PARQUET|TEXT]** SLECT *XXX*; 
**或者**
*tempTableName* = SELECT *XXX*;
INSERT  [OVERWRITE|INTO] TABLE *targetTable*  select * from  *tempTableName*;
INSERT [OVERWRITE|INTO]  DIRECTORY *targetPath*  FORMAT  [JSON|PARQUET|TEXT] select * from *tempTableName*;
**注：** 当结果存入HDFS时，最好显示指定存储格式。默认使用parquet格式。
**示例1：**
```sql
-- 写法1：标准的hql写法，建议使用此种写法。
INSERT into TABLE pbear_market_pg.result_table 
select a.client_no,a.play_date,a.number_of_games,myLen(a.play_date) as date_len 
from play_history as a join uInfo as b on (a.client_no = b.client_no);
    
-- 写法2：将select语句与insert语句拆开写，则需要将select语句的结果赋值给t1，再使用insert方法将t1持久化。
t1 = select a.client_no,a.play_date,a.number_of_games,myLen(a.play_date) as date_len from play_history as a join uInfo as b on (a.client_no = b.client_no);
INSERT into TABLE pbear_market_pg.result_table select * from t1;
```

**示例2：**存在多个select逻辑和多个insert逻辑。
```sql
t1 = select xxx from t0;
insert overwrite table pbear_market_pg.temp_table select a,myUdf(b) from t1;
t2 = select yyy from t1;
insert overwrite table pbear_market_pg.result_table
select xxx,yyy from t1 join t2 on t1.id = t2.id;
```

### 7. 一个简单实例
功能：从PG中读取男性用户，与Hive中表play_history做联合查询，得到男性用户的游戏记录，将结果存入PG表man_play_history中。一个作业中包含多条insert语句。
```sql
CREATE TEMPORARY TABLE uInfo FROM pbear_market_pg WITH "select client_no from user_profile where sex = 0"；

-- 将临时表持久化
-- INSERT OVERWRITE TABLE pbear_market_pg.temp_table select client_no,myUdf(client_no) as hehe from uInfo;

-- 写法1：使用标准hql语法
INSERT OVERWRITE TABLE pbear_market_pg.result_table (select a.client_no,a.play_date,a.number_of_games,myLen(a.play_date) as date_len from play_history as a join uInfo as b on (a.client_no = b.client_no));

-- 写法2：使用拆分写法
-- result_table = select a.client_no,a.play_date,a.number_of_games,myLen(a.play_date) as date_len from play_history as a join uInfo as b on (a.client_no = b.client_no);
--INSERT OVERWRITE TABLE  pbear_market_pg.man_play_history select * from result_table;
```

### 6. 关于数据导入导出
**注：** 需要用户显示指定pbear.batch_size大小，建议值[500000, 3000000],如果此数值设置过大，会被修改为最大值3000000。
```
set pbear.batch_size = 1000000;
```
-----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------

##二. 系统数据流
#### **数据流：**数据源 **-->** 计算引擎 **-->**  结果存储
**数据源 ：**:Kafka，HDFS，RDB，Hive ，ES，MongoDB等
**计算场景：** 数据导入导出Spark Batch(OLAP)，Spark Streamming
**结果存储：**  Kafka，HDFS，RDB，Hive，ES等

-----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------

## 三. 系统实现
### **1.数据导入导出**
**示例：**
```sql
-- 当任务类型为数据导入导出时，必须设置的参数
set batch_size = 1000000;

-- 将BI表user_cube数据导入到Hive中
create temporary table tempTable from pbear_bigdata_mysql with "select client_no,name,age from user_cube";
insert overwrite table bigdata.user_cube select client_no,name,age,myUDF(age) as hehe from tempTable;

-- 将hive表user_cube导出到pg表user_cube
insert overwrite table pbear_market_pg.user_cube
select client_no,name,age from user_cube;
```

### **2.OLAP**
**示例1：**
```sql
-- 运行参数设置
set spark.app.name = "SQL Service Demo";
set spark.executor.memory = 1G;

CREATE TEMPORARY TABLE uInfo FROM pbear_market_pg WITH "select client_no from user_profile where sex = 0"；

CREATE TEMPORARY FUNCTION myLen as 'com.pingan.pbear.commons';
-- 中间结果持久化
INSERT OVERWRITE TABLE pbear_market_pg.temp_table select * from uInfo;

-- 使用标准hql语法执行OLAP任务
INSERT OVERWRITE TABLE pbear_market_pg.temp_table (select a.client_no,a.play_date,a.number_of_games,myLen(a.play_date) as date_len from play_history as a join uInfo as b on (a.client_no = b.client_no));
```
**示例2：**
```sql
-- 注册临时表
CREATE TEMPORARY TABLE raw_news from pbear_market_pg WITH "select news_id,title,summary,content from news_info";

-- 对raw_news表中所有数据进行分词后以overwrite方式存入hive表news_term
-- 其中:pbear.split_word返回值类为array，str_cat功能为将字符串串接在一起
INSERT OVERWRITE TABLE news_term
select news_id,pbear.split_word(str_cat(title,summary,content)) from raw_news;
```
### 3.流计算
**示例：**
```sql
-- 指定数据源，可以为kafka或者hdfs文件，暂时只支持kafka
set streaming.source = kafka;
-- 指定流时间间隔,如：10s,2m，1h等
set streaming.interval = 10s;
-- 显示指定checkpoint地址
set streaming.checkpoint = "/apps/etl/checkpoints/";

-- 若数据源为kafka，则需显示设置是否使用zookeeper.若使用checkpoint方式做容错，则zk_enabled值位false
set kafka.zk_enabled = false;
set kafka.brokers = "10.20.15.219:9092,10.20.15.219:9091";
set kafka.offset = "smallest";
set kafka.topics = "webtrends,news";
-- 为了便于管理，建议设置一个消费组名
set kafka.groups = "pbear_group";
-- 如果使用zookeeper方式做容错，即kafka.zk_enabled = true，则需要显示指定zookeeper地址
-- set kafka.zookeeper = "10.20.15.219:2181";

-- 指定UDF
-- add jar /apps/libs/kafka_user_profile_etl.jar;
add file /apps/libs/parser.py;
-- 将每一条记录使用parser.py处理完后输出
insert into table pbear_market_pg.user_logs
SELECT TRANSFORM(clientNo, userAgent) USING 'python parser.py' FROM user_profile;
```
-----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------
### 四.TODO

| 主要功能模块        | 责任人          |  排期  |
| -------------     |:-------------: | -----:|
| 数据导入导出        |  张润钦         |   ？  |
| OLAP              |  张润钦         |   ?   |
| Streaming         |    ？          |    ?  |
| 机器学习 UDF库      |    ？          |    ?  |
| 基础 UDF库         |    ？          |    ？  |  

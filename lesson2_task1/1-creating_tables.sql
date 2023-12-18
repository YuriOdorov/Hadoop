--ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE odorovju;
SET hive.query.name="partitioning test";
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=120;
SET hive.exec.max.dynamic.partitions.pernode=120;

DROP TABLE IF EXISTS logss;
CREATE EXTERNAL TABLE logss (
    ip STRING,
    dat INT,
    url STRING,
    page_size INT,
    status INT,
    browser STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+(\\d{8})\\d+\\s+((?:https?|ftp):\\/\\/\\S+)\\s+(\\d+)\\s+(\\d{3})\\s+(\\w+\\/.+)$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

DROP TABLE IF EXISTS Logs;
CREATE EXTERNAL TABLE Logs (
    ip STRING,
    url STRING,
    page_size INT,
    status INT,
    browser STRING
)
PARTITIONED BY (dat INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION (dat)
SELECT ip, url, page_size, status, browser, dat FROM logss;
select * from Logs limit 10;

DROP TABLE IF EXISTS Users;
CREATE EXTERNAL TABLE Users (
    ip STRING,
    browser STRING,
    gender STRING,
    age int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';
select * from Users limit 10;

DROP TABLE IF EXISTS IPRegions;
CREATE EXTERNAL TABLE IPRegions (
    ip STRING,
    region STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';
select * from IPRegions limit 10;

DROP TABLE IF EXISTS Subnets;
CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t'
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';
select * from Subnets limit 10;




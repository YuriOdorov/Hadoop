--ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE odorovju;
select dat, count(1) as counter from logs group by dat order by counter desc;
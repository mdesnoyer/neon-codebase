SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS AvroAdPlays;
CREATE EXTERNAL TABLE AvroAdPlays
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS
  INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${hiveconf:table_root}/AdPlayHive'
  TBLPROPERTIES (
    'avro.schema.url'='s3://neon-test/avro-schema/AdPlayHive.avsc'
  );

--The current version of Hive does not support partitions with Avro :-(. 
--It's supposed to be fixed https://issues.cloudera.org/browse/DISTRO-481
--ALTER TABLE AvroAdPlays RECOVER PARTITIONS;

CREATE TABLE AdPlays 
  LIKE AvroAdPlays
  PARTITIONED BY (tai string, ts string)
  as select *, trackerAccountId, to_date(from_unixtime(serverTime)) from avroadplays
  ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
  STORED AS
  INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
  OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat';

create table adplays 
  (pageId string, clientId string, trackerAccountId string, sequenceId bigint) 
  partitioned by (tai string, yr int, mnth int) 
  ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe' 
  STORED AS INPUTFORMAT "parquet.hive.DeprecINPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat' 
  OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat';


insert overwrite table adplays partition(tai, yr, mnth) 
select pageId, clientIp, trackerAccountId, sequenceId, trackerAccountId, year(cast(serverTime as timestamp)), month(cast(serverTime as timestamp)) 
from avroadplays;

select concat_ws('-', year(cast(serverTime as timestamp)), weekofyear(cast(serverTime as timestamp))) from avroadplays limit 2;
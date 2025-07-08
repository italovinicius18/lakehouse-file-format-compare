create schema if not exists iceberg.silver with (location = 's3a://silver/default')
create schema if not exists iceberg.gold with (location = 's3a://gold/default')
create schema if not exists delta.bronze with (location = 's3a://bronze/');
create schema if not exists delta.silver with (location = 's3a://silver/');
create schema if not exists delta.gold with (location = 's3a://gold/');
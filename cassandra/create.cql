CREATE KEYSPACE "data_entities" WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = 'true';

describe cluster;
describe keyspaces;
describe keyspace "data_entities";
describe tables;
describe table data_entities.persons;

USE data_entities;
CREATE TABLE data_entities.persons(
   id double PRIMARY KEY,
   first_name text,
   last_name text,
   email text,
   gender text,
   ip_address text
);

select * from data_entities.persons;

DROP TABLE data_entities.persons;
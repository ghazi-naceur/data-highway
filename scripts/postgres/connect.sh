sudo -u postgres admin

list all users : \du


CREATE USER admin WITH ENCRYPTED PASSWORD 'admin';
ALTER ROLE admin WITH SUPERUSER;


CREATE DATABASE data_entities WITH OWNER admin;
GRANT ALL PRIVILEGES ON DATABASE data_entities TO admin;
\c data_entities admin;



psql -h localhost -U admin -d data_entities

CREATE SCHEMA persons_schema;
CREATE TABLE persons_schema.persons(
 id NUMERIC PRIMARY KEY,
 first_name VARCHAR(50),
 last_name VARCHAR(50),
 email VARCHAR(70),
 gender VARCHAR(10),
 ip_address VARCHAR(100)
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA persons_schema TO admin;

GRANT ALL ON schema persons_schema TO admin;

select * from persons_schema.persons ;

drop table persons_schema.persons;
drop schema persons_schema;
drop table persons;

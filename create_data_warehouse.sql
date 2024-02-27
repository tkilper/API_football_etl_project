-- Create Football API Data Warehouse
create database api_football_db;

create user api_football_user with encrypted password 'tkilper42';

alter database api_football_db owner to api_football_user;
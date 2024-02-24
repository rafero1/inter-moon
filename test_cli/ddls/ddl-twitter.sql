DROP DATABASE index_bc;
DROP DATABASE twitter;
CREATE DATABASE index_bc;
CREATE DATABASE twitter;

\c twitter
CREATE TABLE followers(f1 int NOT NULL DEFAULT '0', f2 int NOT NULL DEFAULT '0', PRIMARY KEY(f1,f2));
CREATE TABLE follows(f1 int NOT NULL DEFAULT '0', f2 int NOT NULL DEFAULT '0', PRIMARY KEY(f1,f2));
CREATE TABLE tweets(id bigint NOT NULL, uid int NOT NULL, text char(140) NOT NULL, createdate timestamp DEFAULT NULL, PRIMARY KEY(id));

\c index_bc
CREATE TABLE user_profiles_index(id varchar, bc_entry varchar);

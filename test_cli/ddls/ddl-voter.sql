DROP DATABASE index_bc;
DROP DATABASE voter;
CREATE DATABASE index_bc;
CREATE DATABASE voter;

\c voter
CREATE TABLE CONTESTANTS(contestant_number integer NOT NULL, contestant_name varchar(50) NOT NULL, PRIMARY KEY(contestant_number));
CREATE TABLE AREA_CODE_STATE(area_code smallint NOT NULL, state varchar(2) NOT NULL, PRIMARY KEY(area_code));

\c index_bc
CREATE TABLE VOTES_index(id varchar, bc_entry varchar);

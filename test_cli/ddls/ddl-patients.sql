DROP DATABASE index_bc;
DROP DATABASE patients;
CREATE DATABASE index_bc;
CREATE DATABASE patients;

\c patients
CREATE TABLE patients(id bigint NOT NULL, name varchar NOT NULL, email varchar NOT NULL, phone varchar NOT NULL, birth_date date NOT NULL, PRIMARY KEY(id));

\c index_bc
CREATE TABLE lab_results_index(id varchar, bc_entry varchar);

/**
 * Tutorial:
 * 1. Create tables for each relational entity
 * 2. Define blockchain assets schema over on mapper/catalog.json
 * 3. Create an index_bc database
 * 4. In index_bc, create the index tables for each blockchain asset (table format: {asset_name}_index (id varchar, bc_entry varchar))
*/
CREATE DATABASE index_bc;

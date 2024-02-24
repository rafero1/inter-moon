#!/bin/bash

su postgres -c 'psql -v ON_ERROR_STOP=1 -1 -f /home/rafael/ddl-reset-lab-results.sql index_bc'

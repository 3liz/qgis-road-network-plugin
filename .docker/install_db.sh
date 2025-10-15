#!/usr/bin/env bash

echo 'Installation from latest version'
psql service=test -c "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;" > /dev/null
psql service=test -f /src/${MODULE_NAME}/install/sql/00_initialize_database.sql > /dev/null
for sql_file in `ls -d -v /src/${MODULE_NAME}/install/sql/${SCHEMA}/*.sql`; do
  echo "${sql_file}"
  psql service=test -f ${sql_file} > /dev/null;
done;

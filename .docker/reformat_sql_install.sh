#!/usr/bin/env bash

echo 'Generating SQL files'
cd /src/${MODULE_NAME}/install/sql/
./export_database_structure_to_SQL.sh test ${SCHEMA}


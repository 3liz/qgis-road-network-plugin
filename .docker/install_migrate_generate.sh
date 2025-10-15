#!/usr/bin/env bash

#
# 1.Install the database with a stored previous version.
# 2.Run the migrations scripts up to the current version.
# 3.Output the migrate schema in tests/.tests-migration-<from>-to-<current>
# 4.Generate a diff `sql.patch` between the current schema and the upgraded one
#

set -eu

current_version=$DB_CURRENT_VERSION
db_version=${DB_INSTALL_VERSION:-$((current_version-1))}

install_dir=/src/tests/data/install-version-$db_version/sql

if [[ ! -e $install_dir ]]; then
    echo "No installation version for database version $db_version"
    exit 1
fi

echo "== Installation from version $db_version"
psql service=test -c "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;" > /dev/null
psql service=test -f $install_dir/00_initialize_database.sql > /dev/null

for sql_file in `ls -d -v $install_dir/${SCHEMA}/*.sql`; do
  echo "${sql_file}"
  psql service=test -f ${sql_file} > /dev/null
done

echo ""

start_migration=$((db_version+1))
echo "== Run SQL migrations stored from version $db_version to $current_version =="
for ((i=start_migration;i<=current_version;i++)); do
   migration=/src/${MODULE_NAME}/install/sql/upgrade/upgrade_to_$i.sql
   if [[ ! -e $migration ]]; then
       echo "Missing migration file $migration"
       exit 1
   fi
   echo "Running migration:  ${migration}"
   psql service=test -f ${migration} > /dev/null;
done;
echo ""

cd /src

echo '== Export update database as installation SQL files'
destination_dir=tests/.test-migration-$db_version-to-$current_version
rm -rf $destination_dir
mkdir -p $destination_dir/sql
pushd $destination_dir/sql
/src/.docker/export_database_structure_to_SQL.sh test ${SCHEMA}
popd


# Generate a diff file between current and install version
echo "== Creating patch file"
set +e  # Suppress exit on erreur
diff -urB $MODULE_NAME/install/sql/$SCHEMA  $destination_dir/sql/$SCHEMA > $destination_dir/sql.patch 

[[ $? -eq 1 ]] && true
echo "== Done"

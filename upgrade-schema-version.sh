#!/bin/bash
#
# Upgrade the schema version and
# prepare migrations files
#
# Must be run with `make upgrade-schema-version`
#
# This scripts does:
#
# 1. Copy sql files from current install to tests/data/install-version-<n>
# 2. Create an empty migration sql file to $MODULE_NAME/install/sql/upgrade/upgrade_to_<n+1>.sql
# 3. Update the schema version in metadata.txt


# Create directories
mkdir -p tests/data $MODULE_NAME/install/sql/upgrade

# Get the actual schema version
schema_version=$(python3 <<EOF
from roadnetwork.plugin_tools import resources
print(resources.schema_version())
EOF
)

if [[ "$schema_version" -eq 0 ]]; then
    echo "No schema version defined"
    exit 1
fi

new_schema_version=$((schema_version+1))

migration_sql=$MODULE_NAME/install/sql/upgrade/upgrade_to_$new_schema_version.sql
if [[ -e $migration_sql ]]; then
    echo "ERROR: the file '$migration_sql' already exists"
    exit 1
fi

echo "Upgrading schema version from '$schema_version' to '$new_schema_version'"

schema_source=$MODULE_NAME/install
schema_dest=tests/data/install-version-$schema_version

new_migration_sql=$MODULE_NAME/install/sql/upgrade/upgrade_to_$new_schema_version.sql

echo "Copying current schema to '$schema_dest'"
cp -ar $schema_source $schema_dest

echo "Creating migration sql file '$new_migration_sql'"
touch $new_migration_sql

echo "Updating schema version in metadadata.txt"
sed -i "s/schemaVersion=.*/schemaVersion=$new_schema_version/" $MODULE_NAME/metadata.txt

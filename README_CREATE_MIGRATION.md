# How to Create a new database installation from migration file

1. Create a new migration scheme:
   ```
   make upgrade-schema-version
   ```
   This copies the installation files of the current version `<n-1>` into `tests/data/install-version-<n-1>/sql`,
   used by the migration tests to install a database of this version.
   The migration files are not copied: they are always read from `roadnetwork/install/sql/upgrade`.
2. Edit the sql migration file; `roadnetwork/install/sql/upgrade/upgrade_to_<n>.sql`
   where `<n>` is the new version number.
3. Run the migration test script:
   ```
   make test-migration
   ```
   This will create a patch file in `tests/.test-migration-<n-1>-to-<n>/sql.patch`
4. Apply the patch file to generate new SQL structure files:
   ```
   make patch-install-files
   ```

# How to Create a new database installation from migration file

1. Create a new migration scheme: 
   ```
   make upgrade-shchema-version
   ```
2. Edit the sql migration file; `roadnetwork/install/sql/upgrade/upgrade_to_<n>.sql`
   where `<n>` is the new version number.
3. Run the migration test script: 
   ```
   make test-migration
   ```
   This will create a patch file in `tests/.test-migration-<n-1>-to-<n>/sql.patch`
4. Apply the patch file to generate new install files:
   ```
   make patch-install-files
   ```

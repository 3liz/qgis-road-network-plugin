
# Overridable
export POSTGIS_VERSION ?= 15-3
export SCHEMA ?= $(shell python3 -m $(MODULE_NAME) default-schema)
export DB_CURRENT_VERSION ?= $(shell python3 -m $(MODULE_NAME) install-version)

# We need to start the database when running tests locally
start-db:
	@echo "Starting database"
	@cd .docker && docker compose up -d --wait

stop-db:
	@cd .docker && docker compose down -v

#
# Create a new migration scheme
#
# 
#

upgrade-schema-version:
	@./upgrade-schema-version.sh

# Apply the patch file from to the migration to the actual install
# sql files
patch-install-files:
	current_version=$$DB_CURRENT_VERSION; \
	db_version=$${DB_INSTALL_VERSION:-$$((current_version-1))}; \
	patch_file=tests/.test-migration-$$db_version-to-$$current_version/sql.patch; \
	echo "== Patch install files to version $current_version"; \
	patch -u -p0 < $$patch_file; \


run-db-command: 
	{ \
		cd .docker; \
		export DB_COMMAND="${DB_COMMAND}"; \
		docker compose --profile=dbrunner up \
			--quiet-pull \
			--abort-on-container-exit \
			--exit-code-from db-runner; \
		docker compose --profile=dbrunner down -v; \
	}

test-migration:
	$(MAKE) run-db-command DB_COMMAND=./install_migrate_generate.sh

reformat-sql:
	$(MAKE) run-db-command  DB_COMMAND="./install_db.sh && ./reformat_sql_install.sh"

schemaspy:
	@rm -rf docs/database/ && mkdir docs/database/
	{ \
		cd .docker; \
		export DB_COMMAND="./install_db_and_wait.sh"; \
		docker compose --profile=schemaspy up  \
			--quiet-pull \
			--abort-on-container-exit \
			--exit-code-from schemaspy; \
		docker compose --profile=schemaspy down -v; \
	}



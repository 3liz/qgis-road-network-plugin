SHELL:=bash

export MODULE_NAME=roadnetwork

-include .localconfig.mk

#
# Configure
#

#
# Configure
#

# Check if uv is available
$(eval UV_PATH=$(shell which uv))
ifdef UV_PATH
ifdef VIRTUAL_ENV
# Always prefer active environment
ACTIVE_VENV=--active
endif
UV=uv run $(ACTIVE_VENV)
endif


REQUIREMENT_GROUPS= \
	dev \
	tests \
	transifex \
	doc \
	$(NULL)


.PHONY: update-requirements

REQUIREMENTS=$(patsubst %, requirements/%.txt, $(REQUIREMENT_GROUPS))

update-requirements: $(REQUIREMENTS)

# Require uv (https://docs.astral.sh/uv/) for extracting
# infos from project's dependency-groups
requirements/%.txt: uv.lock
	@echo "Updating requirements for '$*'"; \
	uv export --format requirements.txt \
		--no-annotate \
		--no-editable \
		--no-hashes \
		--only-group $* \
		-q -o requirements/$*.txt;


#
# Static analysis
#

LINT_TARGETS=$(MODULE_NAME) $(EXTRA_LINT_TARGETS) tests

lint::
	@ $(UV) ruff check  --output-format=concise $(LINT_TARGETS)

lint:: typecheck

lint-fix:
	@ $(UV) ruff check --fix $(LINT_TARGETS)

format:
	@ $(UV) ruff format $(LINT_TARGETS)

typecheck:
	@ $(UV) mypy $(LINT_TARGETS)

scan:
	@ $(UV) bandit -r $(MODULE_NAME) $(SCAN_OPTS) --severity-level=medium


# Database rules
-include database.mk

#
# Tests
#

test::
	$(UV) pytest -v tests

#
# Test using docker image
#

ifdef REGISTRY_URL
REGISTRY_PREFIX=$(REGISTRY_URL)/
else
REGISTRY_PREFIX=3liz/
endif

QGIS_VERSION ?= 3.44
QGIS_IMAGE_REPOSITORY ?= ${REGISTRY_PREFIX}qgis-platform
QGIS_IMAGE_TAG ?= $(QGIS_IMAGE_REPOSITORY):$(QGIS_VERSION)


# Overridable in .localconfig.mk
export QGIS_VERSION
export QGIS_IMAGE_TAG
export UID=$(shell id -u)
export GID=$(shell id -g)


docker-test:
		set -e; \
		export DB_COMMAND=true; \
		cd .docker; \
		docker compose --profile=qgis up \
		--quiet-pull \
		--abort-on-container-exit \
		--exit-code-from qgis; \
		docker compose --profile=qgis  down -v; \


#
# Doc
#

# TODO
#processing-doc:
#	cd .docker && ./processing_doc.sh
#	@docker run --rm -w /plugin -v $(shell pwd):/plugin etrimaille/pymarkdown:latest docs/pro#cessing/README.md docs/processing/index.html
#

#
# Update the project's environment
#
sync:
	@echo "Synchronizing python's environment with frozen dependencies"
	@uv sync --all-groups --frozen $(ACTIVE_VENV)

#
# Coverage
#

# Run tests coverage
covtests:
	@echo "Running coverage tests"
	@ $(UV) coverage run -m pytest tests/

coverage: covtests
	@echo "Building coverage html"
	@ $(UV) coverage html


#
# Code management
#

# Display a summary of codes annotations
show-annotation-%:
	@grep -nR --color=auto --include=*.py '# $*' $(MODULE_NAME)/ -A4 || true

# Output variable
echo-variable-%:
	@echo "$($*)"

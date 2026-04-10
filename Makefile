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


REQUIREMENTS= \
	dev \
	tests \
	transifex \
	doc \
	$(NULL)

.PHONY: uv-required update-requirements

# Require uv (https://docs.astral.sh/uv/) for extracting
# infos from project's dependency-groups
update-requirements: uv.lock
	@for group in $(REQUIREMENTS); do \
		echo "Updating requirements for '$$group'"; \
		uv export --format requirements.txt \
			--no-annotate \
			--no-editable \
			--no-hashes \
			--only-group $$group \
			-q -o requirements/$$group.txt; \
	done

uv.lock: pyproject.toml
	uv sync

update-dependencies: uv.lock
	$(MAKE) update-requirements

#
# Static analysis
#

LINT_TARGETS=$(MODULE_NAME) tests $(EXTRA_LINT_TARGETS)

lint::
	@ $(UV_RUN) ruff check --output-format=concise $(LINT_TARGETS)

lint:: typecheck

lint-preview:
	@ $(UV) ruff check \
	  --output-format=concise \
	  --preview \
	  $(LINT_TARGETS)

lint-fix:
	@ $(UV) ruff check --fix $(LINT_TARGETS)

format:
	@ $(UV) ruff format $(LINT_TARGETS)

typecheck:
	@ $(UV) mypy $(LINT_TARGETS)

scan:
	@ $(UV) bandit -r $(MODULE_NAME) $(SCAN_OPTS) --severity-level=high


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
QGIS_VERSION ?= 3.44
QGIS_IMAGE_REPOSITORY ?= qgis/qgis
QGIS_IMAGE_TAG ?= $(QGIS_IMAGE_REPOSITORY):$(QGIS_VERSION)

# Overridable in .localconfig.mk
export QGIS_VERSION
export QGIS_IMAGE_TAG
export UID=$(shell id -u)
export GID=$(shell id -g)

docker-test:
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
# Update the project's environment
#
sync:
	@echo "Synchronizing python's environment with frozen dependencies"
	@uv sync --all-groups --frozen $(ACTIVE_VENV)

install-dev::
	uv venv --system-site-packages --no-managed-python

install-dev:: sync

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
# Code managment
#

# Display a summary of codes annotations
show-annotation-%:
	@grep -nR --color=auto --include=*.py '# $*' $(MODULE_NAME)/ -A4 || true

# Output variable
echo-variable-%:
	@echo "$($*)"


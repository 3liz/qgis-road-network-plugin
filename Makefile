SHELL:=bash

export MODULE_NAME=roadnetwork

-include .localconfig.mk

#
# Configure
#

ifeq ($(USE_UV), 1)
UV_RUN ?= uv run
endif


REQUIREMENTS= \
	dev \
	tests \
	packaging \
	doc \
	$(NULL)

.PHONY: uv-required update-requirements

# Require uv (https://docs.astral.sh/uv/) for extracting
# infos from project's dependency-groups
update-requirements: check-uv-install
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
	@ $(UV_RUN) ruff check --preview  --output-format=concise $(LINT_TARGETS)

lint:: typecheck

lint-fix:
	@ $(UV_RUN) ruff check --preview --fix $(LINT_TARGETS)

format:
	@ $(UV_RUN) ruff format $(LINT_TARGETS)

typecheck:
	@ $(UV_RUN) mypy $(LINT_TARGETS)

scan:
	@ $(UV_RUN) bandit -r $(MODULE_NAME) $(SCAN_OPTS) --severity-level=high


check-uv-install:
	@which uv > /dev/null || { \
		echo "You must install uv (https://docs.astral.sh/uv/)"; \
		exit 1; \
	}

# Database rules
-include database.mk

#
# Tests
#

test::
	$(UV_RUN) pytest -v tests

#
# Test using docker image
#
QGIS_VERSION ?= 3.40
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
# Code managment
#

# Display a summary of codes annotations
show-annotation-%:
	@grep -nR --color=auto --include=*.py '# $*' $(MODULE_NAME)/ -A4 || true

# Output variable
echo-variable-%:
	@echo "$($*)"

# Running tests

## Running tests in local QGIS environment

You must have an environment with a version of QGIS is installed with
python support (which is available in most of the linux distributions).

Developpement should always take place in a python virtualenv: two ways
of achieving this are supported.

### Using [`uv`](https://docs.astral.sh/uv/)

uv is a great tool for managing dependencies an running tools from a
python virtual env.

First you must [install `uv`](https://docs.astral.sh/uv/getting-started/installation/).

#### Setting up the environment with uv

```
# Create a virtual env with access to system packages (required for using pyQGIS)
> uv venv --system-site-packages
# Update the project's environment
> uv sync --frozen
# Tell make that we are using uv
> echo "USE_UV=1" >> .localconfig.mk
```

Run the tests:

```bash
# First you need to run the database docker container
make start-db
# Run tests
make test
# You can stop the database container
make stop-db
```

It always possible to activate the environment with `. ./.venv/bin/activate` for
using tool command directly (`pytest`, ...) or just run your command with
`uv run <command>`.


### Setting up the environment with python venv and pip

```
# Create a virtual env with access to system packages (required for using pyQGIS)
> python -m venv .venv --system-site-packages
# Activate the environment
> . ./venv/bin/activate
# Update the project's environment
> pip install -r requirements/dev.txt
```

Run the tests:

```bash
# First you need to run the database docker container
make start-db
# Run tests
make test
# You can stop the database container
make stop-db
```


Note that each time you want to do tasks in your environment you will have to
activate the environment.

## Running tests with docker

Tests are run in a docker QGIS image.

```
make docker-test [QGIS_VERSION=<version>]
```

## Building test data

* You first need to create a local PostgreSQL database `road_network`
* Then use the plugin to create the structure with the algorithm `Create database structure`
* Then use the QGIS algorithm `Import data` with the following files:
  * [source_edges.fgb](../roadnetwork/resources/import/source_edges.fgb)
  * [source_markers](../roadnetwork/resources/import/source_markers.fgb)

Once the data has been successfully imported, you can use the `pg_dump` command to create the [SQL test data file](data/sql/test_data.sql):

```bash
pg_dump -d road_network --data-only --disable-triggers --no-owner --inserts -n road_graph -Fp | grep -v "restrict" > tests/data/sql/test_data.sql
```

**NB**: we suppose the test data has been imported in a local database called `road_network`

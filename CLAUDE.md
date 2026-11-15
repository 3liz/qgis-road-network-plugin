# Project instructions

## Before proposing a release (version + tag)

Always run the static checks of the Makefile and make sure they all pass
before proposing a version commit and a tag:

* `make lint` (ruff, then mypy through the `typecheck` target)
* `make typecheck` (mypy)
* `make scan` (bandit, severity medium)

The CI (`.github/workflows/tests.yml`) runs ruff, mypy and bandit on the `roadnetwork` module:
any error there blocks the release.

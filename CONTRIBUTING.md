# Contributing

## Tags, publication, packaging

Tags and version numbers must follow [SemVer convention](https://semver.org/).

To publish a new **release**, you must:

* edit the `pyproject.toml`, change the `version` number and commit
* add a new tag (it can be different that the version written in the file.)
* push to Github (changes and tag)

Every tag **pushed to Github** will trigger a **workflow dispach** which will allow
to create a Github release with a package for this version:

* In the project Github repository, go to the tab `Actions`, menu `Release`,
  press the button `Run workflows` and check the needed options among:
  * Execute dry run
  * Publish to qgis.org
  then `Run workflow`

## Documentation

The documentation is using [MkDocs](https://www.mkdocs.org/) with [Material](https://squidfunk.github.io/mkdocs-material/) :

```bash
pip install -r requirements/doc.txt
mkdocs serve
```

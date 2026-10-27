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

### Screenshots of the Processing algorithms

The screenshots of the Processing algorithm dialogs, used in the
[Installation](https://docs.3liz.org/qgis-road-network-plugin/installation/) page,
are generated automatically from the Python algorithms in `roadnetwork/processing`.

The script `.docker/processing_screenshots.py` opens each algorithm dialog in QGIS,
without any display (Qt `offscreen` platform), in a temporary QGIS profile with a demo
PostgreSQL connection named `road_network_demo`. The images are saved in `docs/media/installation`.

To regenerate them locally, with the QGIS docker image (Docker is required):

```bash
make processing-screenshots
# Or with another QGIS version
make processing-screenshots QGIS_VERSION=3.40
```

To add a new algorithm, add its name and the output file name in the `ALGORITHMS` dictionary
of the script, then reference the image in the documentation.

The GitHub workflow `.github/workflows/processing-screenshots.yml` runs this command whenever
the files in `roadnetwork/processing` are modified on the `main` branch,
and opens a pull request if the screenshots have changed.
It can also be run manually from the `Actions` tab.

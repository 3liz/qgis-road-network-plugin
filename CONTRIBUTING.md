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
[Installation](https://docs.3liz.org/qgis-road-network-plugin/installation/) and
[Import data](https://docs.3liz.org/qgis-road-network-plugin/user-guide/import/) pages,
are generated automatically from the Python algorithms in `roadnetwork/processing`.

The script `.docker/processing_screenshots.py` opens each algorithm dialog in QGIS,
without any display (Qt `offscreen` platform), in a temporary QGIS profile with a demo
PostgreSQL connection named `road_network_demo` and the template layers of `roadnetwork/resources/import`.
The images are saved in `docs/media/installation` and `docs/media/user-guide`.

To regenerate them locally, with the QGIS docker image (Docker is required):

```bash
make processing-screenshots
# Or with another QGIS version
make processing-screenshots QGIS_VERSION=3.40
```

To add a new algorithm, add its name and the output file name (relative to `docs/media`) in the `ALGORITHMS` dictionary
of the script, then reference the image in the documentation.

The GitHub workflow `.github/workflows/processing-screenshots.yml` runs this command whenever
the files in `roadnetwork/processing` are modified on the `main` branch,
and opens a pull request if the screenshots have changed.
It can also be run manually from the `Actions` tab.

### Translation of the documentation

The documentation is written in English and translated on [Transifex](https://app.transifex.com/3liz-1/road-network/),
in the same project as the plugin interface. Each page is a resource named `docs-<path of the page>`,
for example `docs-user-guide-graph-checks`, with the Markdown file format:
Transifex splits it into strings (paragraphs, headings, list items, table cells),
and keeps the translations of the strings which have not changed.

* The English pages are sent to Transifex by the GitHub workflow `.github/workflows/transifex.yml`
  when the pages are modified on the `main` branch.
* The translated pages are downloaded by the GitHub workflow `.github/workflows/publish-doc.yml`
  before building the documentation, every time it is published and every Monday.
  They are saved next to the English pages, with the language as suffix (`graph-checks.fr.md`),
  and are not versioned. The pages with less than 50 % of translated strings are displayed in English.
* The MkDocs plugin [mkdocs-static-i18n](https://ultrabug.github.io/mkdocs-static-i18n/) builds
  the site of each language (`/fr/` for French), with a language selector.
  The titles of the menu are translated in `mkdocs.yml` (`nav_translations`).
* The hook `docs_tools/i18n_anchors.py` gives to the headings of the translated pages the anchors
  of the English headings, so that the links to a section (`graph-checks.md#g01-crossing-edges-without-node`)
  work in every language.

The list of the translated pages is in `docs_tools/transifex_docs.py`.
Translators must keep the Markdown syntax, the URLs of the links and the code unchanged.

To work locally, with the Transifex API token in the environment variable `TRANSIFEX_TOKEN`:

```bash
# Send the English pages
make doc-transifex-push
# Percentage of translated and reviewed strings of each page
make doc-transifex-status
# Download the translated pages, then display the site
make doc-transifex-pull
mkdocs serve
```

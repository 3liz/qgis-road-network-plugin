#!/usr/bin/env python3
"""Send the English pages of the documentation to Transifex, and get back their translations.

Each page is a resource of the Transifex project of the plugin, with the Markdown file format
(GITHUBMARKDOWN): Transifex splits it into strings (paragraphs, headings, list items, table cells),
and keeps the translations of the strings which have not changed when the page is sent again.

The translations are saved next to the English pages, with the language as suffix
(for example docs/user-guide/graph-checks.fr.md), as expected by the MkDocs plugin mkdocs-static-i18n.
They are not versioned: they are downloaded before building the documentation.

The texts of the diagrams of the road graph checks (.docker/graph_checks_diagrams.py) are another resource,
docs-graph-checks-diagrams, with the key-value JSON file format: for each language, the translated diagrams
are written next to the English ones (for example docs/media/user-guide/graph_checks_g01.fr.svg).

The organization and the project are the ones of the plugin interface, in the section
[tool.qt-transifex] of pyproject.toml. The Transifex API token is given by the environment
variable TRANSIFEX_TOKEN, or by the option --transifex-token.

Usage, from the repository root:

    python3 docs_tools/transifex_docs.py push
    python3 docs_tools/transifex_docs.py pull [--minimum-translation 50]
    python3 docs_tools/transifex_docs.py status
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import tomllib

from pathlib import Path

import requests

from transifex.api import transifex_api as tx
from transifex.api.jsonapi.exceptions import DoesNotExist

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# Pages to translate, relative to the docs directory
# Not translated: changelog.md and contributing.md (copied from the repository root),
# appendix/training-plan.md (already in French), database-guide/structure.md (generated documentation)
PAGES = (
    "index.md",
    "concepts/index.md",
    "installation/index.md",
    "user-guide/index.md",
    "user-guide/import.md",
    "user-guide/qgis-project.md",
    "user-guide/processing.md",
    "user-guide/graph-editing.md",
    "user-guide/graph-checks.md",
    "user-guide/managed-objects.md",
    "database-guide/functions.md",
    "database-guide/tables.md",
    "lizmap-web-client/index.md",
)
FILE_FORMAT = "GITHUBMARKDOWN"
SOURCE_LANGUAGE = "en"

# Texts of the diagrams of the road graph checks
DIAGRAMS_SCRIPT = ROOT / ".docker" / "graph_checks_diagrams.py"
DIAGRAMS_DIR = DOCS / "media" / "user-guide"
DIAGRAMS_RESOURCE = "docs-graph-checks-diagrams"
DIAGRAMS_FILE_FORMAT = "KEYVALUEJSON"


def resource_slug(page: str) -> str:
    """Slug of the Transifex resource of a page.

    :param page: The path of the page, relative to the docs directory
    :return: The slug, for example docs-user-guide-graph-checks
    """
    return "docs-" + page.removesuffix(".md").replace("/", "-")


def translated_page(page: str, language: str) -> Path:
    """Path of the translation of a page.

    :param page: The path of the page, relative to the docs directory
    :param language: The language code
    :return: The path of the translated page, for example docs/user-guide/graph-checks.fr.md
    """
    return DOCS / f"{page.removesuffix('.md')}.{language}.md"


def get_project(token: str) -> tx.Project:
    """Transifex project of the plugin, given in the section [tool.qt-transifex] of pyproject.toml.

    :param token: The Transifex API token
    :return: The Transifex project
    """
    with (ROOT / "pyproject.toml").open("rb") as stream:
        config = tomllib.load(stream)["tool"]["qt-transifex"]
    tx.setup(auth=token)
    organization = tx.Organization.get(slug=config["organization"])
    return organization.fetch("projects").get(slug=config["project"])


def get_resources(project: tx.Project) -> dict:
    """Existing resources of the project, by slug.

    :param project: The Transifex project
    :return: The resources
    """
    return {resource.slug: resource for resource in project.fetch("resources").all()}


def get_or_create_resource(
    project: tx.Project, resources: dict, slug: str, name: str, file_format: str
) -> tx.Resource:
    """Resource of the project, created if it does not exist yet.

    :param project: The Transifex project
    :param resources: The existing resources of the project, by slug
    :param slug: The slug of the resource
    :param name: The name of the resource, used to create it
    :param file_format: The file format of the resource, used to create it
    :return: The resource
    """
    resource = resources.get(slug)
    if resource is None:
        resource = tx.Resource.create(
            project=project,
            name=name,
            slug=slug,
            i18n_format=tx.I18nFormat(id=file_format),
        )
        print(f"Resource {slug} created")  # noqa: T201
    return resource


def diagrams_texts() -> str:
    """English texts of the diagrams of the road graph checks, extracted by the script of the diagrams.

    :return: The JSON content {text: text}
    """
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "texts.json"
        subprocess.run([sys.executable, str(DIAGRAMS_SCRIPT), "--extract", str(path)], check=True)
        return path.read_text(encoding="utf-8")


def write_diagrams(language: str, translations: bytes | None):
    """Write the diagrams of the road graph checks in a language, or delete them.

    :param language: The language code
    :param translations: The JSON content of the translations {text: translation}, None to delete the diagrams
    """
    if translations is None:
        for path in DIAGRAMS_DIR.glob(f"graph_checks_g*.{language}.svg"):
            path.unlink()
        return
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / f"texts.{language}.json"
        # Untranslated texts are empty strings: the English texts are kept
        path.write_text(json.dumps(json.loads(translations), ensure_ascii=False), encoding="utf-8")
        command = [sys.executable, str(DIAGRAMS_SCRIPT), str(DIAGRAMS_DIR), "--language", language]
        subprocess.run([*command, "--translations", str(path)], check=True)


def translation_ratio(project: tx.Project, resource: tx.Resource) -> dict:
    """Percentage of translated strings of a resource, by language.

    :param project: The Transifex project
    :param resource: The resource
    :return: The percentage of each language code
    """
    ratios = {}
    for stat in tx.ResourceLanguageStats.filter(project=project, resource=resource):
        total = stat.attributes["total_strings"]
        translated = stat.attributes["translated_strings"]
        ratios[stat.id.partition(":l:")[2]] = 100 * translated / total if total else 0
    return ratios


def download(resource: tx.Resource, language: tx.Language) -> bytes:
    """Translation file of a resource in a language.

    :param resource: The resource
    :param language: The language
    :return: The content of the file
    """
    url = tx.ResourceTranslationsAsyncDownload.download(resource=resource, language=language)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def push(project: tx.Project) -> int:
    """Send the English pages and the texts of the diagrams to Transifex, creating their resources if needed.

    :param project: The Transifex project
    :return: The exit code
    """
    resources = get_resources(project)
    for page in PAGES:
        resource = get_or_create_resource(
            project, resources, resource_slug(page), f"Documentation - {page}", FILE_FORMAT
        )
        content = (DOCS / page).read_text(encoding="utf-8")
        result = tx.ResourceStringsAsyncUpload.upload(content, resource=resource)
        print(f"{page}: {result}")  # noqa: T201

    # Texts of the diagrams
    resource = get_or_create_resource(
        project, resources, DIAGRAMS_RESOURCE, "Documentation - graph checks diagrams", DIAGRAMS_FILE_FORMAT
    )
    result = tx.ResourceStringsAsyncUpload.upload(diagrams_texts(), resource=resource)
    print(f"Diagrams: {result}")  # noqa: T201
    return 0


def pull(project: tx.Project, minimum_translation: float) -> int:
    """Download the translations of the pages and of the diagrams, for each language of the project.

    The pages and the diagrams with less translated strings than the minimum are deleted:
    the English ones are displayed instead.

    :param project: The Transifex project
    :param minimum_translation: Minimum percentage of translated strings to download the translation of a page
    :return: The exit code
    """
    resources = get_resources(project)
    languages = list(project.fetch("languages").all())
    for page in PAGES:
        resource = resources.get(resource_slug(page))
        if resource is None:
            print(f"{page}: no resource, run the command push first", file=sys.stderr)  # noqa: T201
            continue
        ratios = translation_ratio(project, resource)
        for language in languages:
            ratio = ratios.get(language.code, 0)
            path = translated_page(page, language.code)
            if ratio < minimum_translation:
                print(f"{page}: {language.code} {ratio:.0f} %, not downloaded")  # noqa: T201
                path.unlink(missing_ok=True)
                continue
            path.write_bytes(download(resource, language))
            print(f"{page}: {language.code} {ratio:.0f} % -> {path.relative_to(ROOT)}")  # noqa: T201

    # Diagrams written with the translated texts
    resource = resources.get(DIAGRAMS_RESOURCE)
    if resource is None:
        print("Diagrams: no resource, run the command push first", file=sys.stderr)  # noqa: T201
        return 0
    ratios = translation_ratio(project, resource)
    for language in languages:
        ratio = ratios.get(language.code, 0)
        if ratio < minimum_translation:
            print(f"Diagrams: {language.code} {ratio:.0f} %, not translated")  # noqa: T201
            write_diagrams(language.code, None)
            continue
        write_diagrams(language.code, download(resource, language))
        print(f"Diagrams: {language.code} {ratio:.0f} % -> {DIAGRAMS_DIR.relative_to(ROOT)}")  # noqa: T201
    return 0


def status(project: tx.Project) -> int:
    """Print the percentage of translated and reviewed strings of each page and language.

    :param project: The Transifex project
    :return: The exit code
    """
    resources = get_resources(project)
    for name, slug in [*((page, resource_slug(page)) for page in PAGES), ("Diagrams", DIAGRAMS_RESOURCE)]:
        resource = resources.get(slug)
        if resource is None:
            print(f"{name}: no resource")  # noqa: T201
            continue
        for stat in tx.ResourceLanguageStats.filter(project=project, resource=resource):
            language = stat.id.partition(":l:")[2]
            if language == SOURCE_LANGUAGE:
                continue
            total = stat.attributes["total_strings"] or 1
            print(  # noqa: T201
                f"{name}: {language} translated {100 * stat.attributes['translated_strings'] / total:.0f} %, "
                f"reviewed {100 * stat.attributes['reviewed_strings'] / total:.0f} %"
            )
    return 0


def main() -> int:
    """Run the command given on the command line.

    :return: The exit code
    """
    parser = argparse.ArgumentParser(description="Translation of the documentation with Transifex")
    parser.add_argument("command", choices=("push", "pull", "status"))
    parser.add_argument("--transifex-token", default=os.environ.get("TRANSIFEX_TOKEN"))
    parser.add_argument(
        "--minimum-translation",
        type=float,
        default=50,
        help="Minimum percentage of translated strings to download the translation of a page (default 50)",
    )
    args = parser.parse_args()
    if not args.transifex_token:
        parser.error("the Transifex API token is required: TRANSIFEX_TOKEN or --transifex-token")

    try:
        project = get_project(args.transifex_token)
    except DoesNotExist:
        print("The Transifex organization or project has not been found", file=sys.stderr)  # noqa: T201
        return 1
    if args.command == "push":
        return push(project)
    if args.command == "pull":
        return pull(project, args.minimum_translation)
    return status(project)


if __name__ == "__main__":
    sys.exit(main())

"""MkDocs hook keeping the anchors of the English headings in the translated pages.

The translated pages (for example graph-checks.fr.md, next to graph-checks.md) are downloaded from Transifex.
Their headings are translated, so the anchors generated from them would differ from the English ones,
and the links to these anchors (for example graph-checks.md#g01-crossing-edges-without-node) would be broken.

This hook gives to each heading of a translated page the anchor of the heading at the same position
in the English page, with the attribute list syntax `## Title { #anchor }`.
The translators do not have to know the anchors: Transifex keeps the headings in the same order.
"""

import logging
import re
import unicodedata

from pathlib import Path

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page

LOGGER = logging.getLogger("mkdocs.hooks.i18n_anchors")

# Heading of a Markdown line, with its level and its text
HEADING = re.compile(r"^(#{1,6})[ \t]+(.+?)[ \t]*#*[ \t]*$")
# Explicit anchor given with the attribute list syntax
EXPLICIT_ANCHOR = re.compile(r"\{[^}]*#([\w.:-]+)[^}]*\}\s*$")
# Start or end of a fenced code block
FENCE = re.compile(r"^\s*(```|~~~)")
# Translated page name: name.<language>.md
TRANSLATED_PAGE = re.compile(r"^(.+)\.([a-z]{2}(?:_[A-Z]{2})?)\.md$")


def slugify(text: str) -> str:
    """Anchor of a heading, like the default slugify function of the Python Markdown toc extension.

    The inline Markdown syntax (code, emphasis, links, HTML tags) is removed first,
    since the toc extension uses the rendered text of the heading.

    :param text: The Markdown text of the heading
    :return: The anchor
    """
    # Rendered text of the heading
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[`*]|(?<!\w)_|_(?!\w)", "", text)
    # Same steps as markdown.extensions.toc.slugify
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def headings(markdown: str) -> list[tuple[int, str]]:
    """Headings of a Markdown page, outside the fenced code blocks.

    :param markdown: The Markdown content of the page
    :return: The line number and the text of each heading
    """
    result = []
    in_fence = False
    for number, line in enumerate(markdown.splitlines()):
        # Lines starting with # in code blocks are not headings
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        match = HEADING.match(line)
        if not in_fence and match:
            result.append((number, match.group(2)))
    return result


def anchors(markdown: str) -> list[str]:
    """Anchors of the headings of a Markdown page, made unique like the toc extension does.

    :param markdown: The Markdown content of the page
    :return: The anchor of each heading, in the order of the page
    """
    result: list[str] = []
    for _, text in headings(markdown):
        explicit = EXPLICIT_ANCHOR.search(text)
        anchor = explicit.group(1) if explicit else slugify(text)
        # Same anchor as a previous heading: the toc extension adds _1, _2, etc.
        unique = anchor
        index = 1
        while unique in result:
            unique = f"{anchor}_{index}"
            index += 1
        result.append(unique)
    return result


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files) -> str:
    """Give the anchors of the English page to the headings of a translated page.

    :param markdown: The Markdown content of the page
    :param page: The page
    :param config: The MkDocs configuration
    :param files: The files of the site
    :return: The Markdown content, with explicit anchors for the headings of a translated page
    """
    source = Path(page.file.abs_src_path or "")
    match = TRANSLATED_PAGE.match(source.name)
    if not match:
        return markdown

    # English page next to the translated page
    english = source.with_name(f"{match.group(1)}.md")
    if not english.exists():
        return markdown
    english_anchors = anchors(english.read_text(encoding="utf-8"))
    translated_headings = headings(markdown)
    if len(english_anchors) != len(translated_headings):
        LOGGER.warning(
            "%s: %s headings instead of %s in the English page, the anchors are not kept",
            source.name,
            len(translated_headings),
            len(english_anchors),
        )
        return markdown

    # Add the English anchor to each heading which has no explicit anchor yet
    lines = markdown.splitlines()
    for (number, text), anchor in zip(translated_headings, english_anchors):
        if not EXPLICIT_ANCHOR.search(text):
            lines[number] = f"{lines[number].rstrip()} {{ #{anchor} }}"
    return "\n".join(lines) + ("\n" if markdown.endswith("\n") else "")

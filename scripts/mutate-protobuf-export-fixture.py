#!/usr/bin/env python3

import argparse
from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    source = path.read_text()
    count = source.count(old)
    if count != 1:
        raise SystemExit(
            f"expected exactly one mutation anchor in {path}, found {count}"
        )
    path.write_text(source.replace(old, new, 1))


def add_column(path: Path) -> None:
    anchor = '\t//ptah:schema:index name="idx_categories_parent" fields="parent_id"'
    added = (
        '\t//ptah:schema:field name="tagline" type="VARCHAR(200)" '
        'not_null="false"\n'
        "\tTagline string\n\n"
    )
    replace_once(path, anchor, added + anchor)


def reorder_columns(path: Path) -> None:
    first = (
        '\t//ptah:schema:field name="name" type="VARCHAR(100)" '
        'not_null="true" unique="true"\n'
        "\tName string\n\n"
    )
    second = (
        '\t//ptah:schema:field name="slug" type="VARCHAR(100)" '
        'not_null="true" unique="true"\n'
        "\tSlug string\n\n"
    )
    replace_once(path, first + second, second + first)


def remove_column(path: Path) -> None:
    field = (
        '\t//ptah:schema:field name="display_order" type="INT" '
        'not_null="true" default_expr="0"\n'
        "\tDisplayOrder int\n\n"
    )
    replace_once(path, field, "")


def change_column_type(path: Path) -> None:
    replace_once(
        path,
        'name="display_order" type="INT"',
        'name="display_order" type="TEXT"',
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=(
            "add-column",
            "reorder-columns",
            "remove-column",
            "remove-type",
            "change-type",
        ),
    )
    parser.add_argument("stubs_dir", type=Path)
    args = parser.parse_args()

    category = args.stubs_dir / "category.go"
    mutations = {
        "add-column": lambda: add_column(category),
        "reorder-columns": lambda: reorder_columns(category),
        "remove-column": lambda: remove_column(category),
        "remove-type": category.unlink,
        "change-type": lambda: change_column_type(category),
    }
    mutations[args.mode]()


if __name__ == "__main__":
    main()

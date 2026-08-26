#!/usr/bin/env bash
# Requires the site header logo and the browser favicon to hold the same
# artwork, and requires the mark to stay legible at favicon size.
#
# The two files are separate because Starlight loads one through the Astro
# asset pipeline and the browser fetches the other from the site root. Nothing
# connects them, so an edit to one is silently an edit to only one, and the tab
# icon drifts away from the header at a size nobody looks at closely.
#
# The gap rule is the legibility one, and it is arithmetic rather than taste: a
# browser rasterizing the 64-unit grid into 16 CSS pixels turns four grid units
# into one pixel, so courses closer than three units merge and the mark becomes
# a blob. Both rules are stated in docs/site/README.md; this is what keeps them
# true (stokaro/ptah#493).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

logo="docs/site/src/assets/logo.svg"
favicon="docs/site/public/favicon.svg"

for path in "$logo" "$favicon"; do
	if [ ! -f "$path" ]; then
		echo "check-brand-assets: ${path} is missing" >&2
		exit 1
	fi
done

# The accessible name is the one difference the two files are allowed to have:
# the header logo is an image in the page and is announced, the tab icon is not.
if ! grep -q '<title id="title">Ptah</title>' "$logo"; then
	echo "check-brand-assets: ${logo} has to carry its <title>Ptah</title>" >&2
	echo "  it is announced to screen readers as the site's home link" >&2
	exit 1
fi

python3 - "$logo" "$favicon" <<'PY'
import re
import sys

logo_path, favicon_path = sys.argv[1], sys.argv[2]

RECT = re.compile(r"<rect\b([^>]*?)/?>")
ATTR = re.compile(r'([a-zA-Z-]+)\s*=\s*"([^"]*)"')


def rects(path):
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    return [dict(ATTR.findall(body)) for body in RECT.findall(source)]


def drawing(path):
    # Sorted pairs, so reordering attributes is not reported as a difference.
    return [tuple(sorted(rect.items())) for rect in rects(path)]


failures = []

logo_drawing, favicon_drawing = drawing(logo_path), drawing(favicon_path)
if logo_drawing != favicon_drawing:
    failures.append(
        f"{logo_path} and {favicon_path} draw different artwork\n"
        f"  header : {logo_drawing}\n"
        f"  favicon: {favicon_drawing}\n"
        "  the tab icon and the site header have to be the same mark"
    )

# A rect with neither x nor y is the background tile, not a course.
for path in (logo_path, favicon_path):
    courses = [r for r in rects(path) if "x" in r and "y" in r and "height" in r]
    if len(courses) < 2:
        failures.append(f"{path} draws fewer than two courses; the mark is not this shape")
        continue
    courses.sort(key=lambda r: float(r["y"]))
    for above, below in zip(courses, courses[1:]):
        gap = float(below["y"]) - (float(above["y"]) + float(above["height"]))
        if gap < 3:
            failures.append(
                f"{path} leaves {gap:g} grid units between the course at y={above['y']} "
                f"and the one at y={below['y']}\n"
                "  three units is one pixel at favicon size; closer than that and they merge"
            )

if failures:
    for failure in failures:
        print(f"check-brand-assets: {failure}", file=sys.stderr)
    raise SystemExit(1)
PY

echo "check-brand-assets: OK (header logo and favicon draw the same mark, courses stay apart)"

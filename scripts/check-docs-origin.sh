#!/usr/bin/env bash
# The documentation site's address is declared once, and the retired one is gone.
#
# Ptah's documentation moved from a GitHub project page to its own apex domain.
# The address was not written down once: it was a `site` literal in
# `docs/site/astro.config.mjs`, a `base` literal beside it, a differently-shaped
# `PAGES_PREFIX` in `docs/site/scripts/gen-versions.mjs`, a third one in
# `check-pages-root.mjs`, and a regexp in that same file -- plus prose,
# installers and workflows. Moving some of them produced a site that was
# published at the new host and addressed at the old one: the apex stub sent
# every reader to `/ptah/<version>/`, which is 404, and the page that did exist
# loaded no styles because each asset href still began `/ptah/`
# (stokaro/ptah#2884).
#
# So there are two rules, and neither is the other:
#
#   1. The retired host must not come back, anywhere in the tracked tree.
#   2. Inside `docs/site`'s JavaScript, the live origin is imported from
#      `src/lib/docs-origin.mjs` rather than spelled again. That corpus is where
#      the duplication caused the outage, and it is the corpus where importing
#      the declaration is always possible. Prose, the standalone installers and
#      the workflows spell the address by necessity -- a Markdown page and a
#      `curl | sh` one-liner cannot import an ES module -- so rule 2 does not
#      reach them and rule 1 is what holds them. The same rule holds the project
#      site's origin, which the same file declares: the installers are
#      advertised there, and a second spelling of it is the same defect.
#
# This gate contains no literal spelling of either host. The needle is assembled
# from halves below, which is not obfuscation: a scanner that names what it
# forbids matches itself, and the alternative is an exemption list that has to
# be maintained and that silently excuses whatever gets added to it. Assembling
# the string lets the scan cover every tracked file including this one.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# The retired GitHub project page, and the project prefix it served under.
retired_host="stokaro.github$(printf '\056')io"
# The declaration every JavaScript caller must read instead of spelling it.
declaration="docs/site/src/lib/docs-origin.mjs"
live_host="docs.ptah$(printf '\056')run"
# The project site, scheme included: its host is a suffix of the live host, and
# the scheme is what keeps this needle from matching every documentation URL.
site_origin="https://ptah$(printf '\056')run"

# corpus is every file this gate reads, and it asks git rather than walking the
# filesystem for the reason scripts/check-test-style.sh documents: a walk
# descends into the linked worktrees parked under this repository and reports a
# different checkout's files as this one's.
#
# `--others --exclude-standard` is what makes it the working tree rather than
# the index. A file added in this change is untracked until it is staged, so a
# gate reading `git ls-files` alone cannot see the one file most likely to be
# wrong -- and it does not fail, it reports success. Measured: this gate ran
# green over its own declaration while that declaration still spelled the
# retired host, and CI, which reads a committed tree, was the first thing to
# disagree. `--exclude-standard` is also what keeps the sibling worktrees out,
# since the directories holding them are ignored.
corpus() {
	git ls-files --cached --others --exclude-standard "$@"
}

status=0

# Rule 1. The retired host, anywhere.
retired_hits="$(corpus -z | xargs -0 grep -nF -- "$retired_host" 2>/dev/null || true)"
if [ -n "$retired_hits" ]; then
	echo "check-docs-origin: the retired documentation host is still spelled here:" >&2
	echo "$retired_hits" | sed 's/^/  /' >&2
	echo "  the site is published at the apex domain now; see stokaro/ptah#2884" >&2
	status=1
fi

# Rule 2. The live origin, spelled inside docs/site JavaScript that could import
# it. The declaration itself is where it is allowed to appear, and it is
# excluded by name rather than by pattern so that renaming it fails here loudly.
if [ ! -f "$declaration" ]; then
	echo "check-docs-origin: ${declaration} is missing; nothing declares the site's address" >&2
	exit 1
fi

js_hits=""
while IFS= read -r path; do
	[ "$path" = "$declaration" ] || js_hits="${js_hits}$(grep -nF -e "$live_host" -e "$site_origin" -- "$path" /dev/null || true)
"
done < <(corpus -- 'docs/site/*.mjs' 'docs/site/*.js' 'docs/site/*.ts')
js_hits="$(printf '%s' "$js_hits" | grep -v '^$' || true)"

if [ -n "$js_hits" ]; then
	echo "check-docs-origin: the site's address is spelled again instead of imported:" >&2
	echo "$js_hits" | sed 's/^/  /' >&2
	echo "  import { Origin } or { SiteOrigin } from '<relative path>/src/lib/docs-origin.mjs' and build from it." >&2
	echo "  Two literals of this address is what published a site nobody could style" >&2
	echo "  (stokaro/ptah#2884)." >&2
	status=1
fi

# A scan that matches nothing reports success at exactly the moment it stopped
# reading. Both rules are negative, so neither can count its own findings as
# evidence of having run -- what proves the corpus was non-empty is the corpus.
tracked_js="$(corpus -- 'docs/site/*.mjs' 'docs/site/*.js' 'docs/site/*.ts' | wc -l | tr -d ' ')"
tracked_all="$(corpus | wc -l | tr -d ' ')"
if [ "$tracked_js" -lt 10 ] || [ "$tracked_all" -lt 100 ]; then
	echo "check-docs-origin: read ${tracked_all} working-tree files and ${tracked_js} docs/site scripts;" >&2
	echo "  that is too few to be this repository, so the scan is not reporting on it" >&2
	exit 1
fi

if [ "$status" -ne 0 ]; then
	exit 1
fi

echo "check-docs-origin: OK (${tracked_all} working-tree files carry no retired host;" \
	"${tracked_js} docs/site scripts read the address from ${declaration})"

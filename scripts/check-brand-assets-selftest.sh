#!/usr/bin/env bash
# Proves check-brand-assets.sh reports drift between the two files and a mark
# that stops being legible at favicon size.
#
# The two files are separate because Starlight loads one through the Astro asset
# pipeline and the browser fetches the other from the site root. Nothing
# connects them, so an edit to one is silently an edit to only one, and the tab
# icon drifts away from the header at a size nobody looks at closely
# (stokaro/ptah#493; stokaro/ptah#2509 moves the fixtures here).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-brand-assets.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-brand-assets.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# mark renders one artwork. The courses are the horizontal bars; y and height
# are what the legibility rule is arithmetic over.
mark() {
	local title=$1 fill=$2 second_y=$3
	printf '<svg viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">\n'
	[ -n "$title" ] && printf '  %s\n' "$title"
	printf '  <rect width="64" height="64" fill="#0b1220"/>\n'
	printf '  <rect x="12" y="14" width="40" height="8" fill="%s"/>\n' "$fill"
	printf '  <rect x="12" y="%s" width="40" height="8" fill="%s"/>\n' "$second_y" "$fill"
	printf '</svg>\n'
}

write_repo() {
	local logo=$1 favicon=$2
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/docs/site/src/assets" "$work_dir/repo/docs/site/public"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-brand-assets.sh"
	printf '%s' "$logo" >"$work_dir/repo/docs/site/src/assets/logo.svg"
	printf '%s' "$favicon" >"$work_dir/repo/docs/site/public/favicon.svg"
}

assert_rejected() {
	local name=$1 expected=$2
	if (cd "$work_dir/repo" && scripts/check-brand-assets.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'brand asset self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'brand asset self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	local name=$1
	if ! (cd "$work_dir/repo" && scripts/check-brand-assets.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'brand asset self-test: %s was rejected:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

titled='<title id="title">Ptah</title>'
good_logo="$(mark "$titled" '#f59e0b' 30)"
good_favicon="$(mark '' '#f59e0b' 30)"

# The control first, so every rejection below is a difference from something the
# gate accepts rather than from a fixture it never liked.
write_repo "$good_logo" "$good_favicon"
assert_accepted 'two files drawing one mark, courses eight units apart'

# The drift the two files exist to make possible: one edited, one not.
write_repo "$good_logo" "$(mark '' '#38bdf8' 30)"
assert_rejected 'the favicon recoloured on its own' 'draw different artwork'

# The legibility rule, and it is arithmetic rather than taste: a browser
# rasterizing the 64-unit grid into 16 pixels turns four units into one, so
# courses closer than three units merge into a blob.
write_repo "$(mark "$titled" '#f59e0b' 24)" "$(mark '' '#f59e0b' 24)"
assert_rejected 'courses two units apart' 'three units is one pixel at favicon size'

# The boundary: exactly three units apart is the closest the rule allows, and a
# fixture at the limit is what separates "reads the gap" from "rejects anything
# it has not seen".
write_repo "$(mark "$titled" '#f59e0b' 25)" "$(mark '' '#f59e0b' 25)"
assert_accepted 'courses exactly three units apart'

# The accessible name is the one difference the two files are allowed to have,
# and the header logo is the one that must carry it.
write_repo "$(mark '' '#f59e0b' 30)" "$good_favicon"
assert_rejected 'a header logo with no title' 'has to carry its <title>Ptah</title>'

# A file that is not there at all.
write_repo "$good_logo" "$good_favicon"
rm "$work_dir/repo/docs/site/public/favicon.svg"
assert_rejected 'a missing favicon' 'favicon.svg is missing'

printf 'brand asset self-test: drift, an illegible gap, a missing title and a missing file are each reported, and the three-unit boundary is accepted\n'

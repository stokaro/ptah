#!/usr/bin/env bash

set -euo pipefail

# Prints every opt-in build tag the tracked Go files use, one per line, sorted.
# With --joined it prints them comma-separated, the form -tags and
# --build-tags take.
#
# An opt-in tag is a name in a //go:build constraint that the go tool never sets
# by itself: not a GOOS or GOARCH (from `go tool dist list`), not unix, cgo, gc,
# gccgo, a go1.N release tag or a goexperiment tag, and not ignore, which marks
# a file no build compiles.
#
# The lint contours are built from this list. The default contour sets no tag,
# and the tagged contour sets every tag here at once. Between them they read
# every file, provided no file needs one opt-in tag while excluding another:
# such a file is in neither build. So a constraint that names two opt-in tags
# and negates anything is refused here, before a contour silently misses it.
#
# The list is discovered for the reason scripts/list-go-modules.sh gives. A
# written list of tags covers the tags someone remembered, and a tag no
# workflow or Makefile target names is compiled by nothing (stokaro/ptah#3895).
#
# Files under testdata/ are left out. The go tool never builds them, so a tag
# they carry (testcontour_fixture) selects nothing a contour could read. The
# files come from `git ls-files`, never a walk, for the reason
# scripts/check-test-style.sh gives.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

joined=0
case "${1:-}" in
"") ;;
--joined) joined=1 ;;
*)
	printf 'usage: %s [--joined]\n' "$0" >&2
	exit 2
	;;
esac

platforms="$(go tool dist list | tr '/' '\n' | sort -u | tr '\n' ' ')"

# The constraint is the //go:build line before the package clause. Go file names
# here never hold a newline or a space (revive's filename-format rule), so a
# line list is safe to hand to xargs.
tags="$(
	git ls-files '*.go' | { grep -Ev '(^|/)testdata/' || true; } | tr '\n' '\0' |
		xargs -0 -r awk '/^package /{nextfile} /^\/\/go:build /{sub(/^\/\/go:build /, ""); print FILENAME "\t" $0}' |
		awk -F '\t' -v platforms="$platforms" '
		BEGIN {
			n = split(platforms " unix cgo gc gccgo ignore", names, " ")
			for (i = 1; i <= n; i++) {
				builtin[names[i]] = 1
			}
		}
		{
			words = $2
			gsub(/[^A-Za-z0-9_.]+/, " ", words)
			k = split(words, word, " ")
			split("", named)
			count = 0
			for (i = 1; i <= k; i++) {
				w = word[i]
				if (w == "" || (w in builtin) || w ~ /^go1\.[0-9]+$/ || w ~ /^goexperiment\./) {
					continue
				}
				if (!(w in named)) {
					named[w] = 1
					count++
				}
				found[w] = 1
			}
			if (count >= 2 && index($2, "!") > 0) {
				printf "%s: //go:build %s names two opt-in tags and negates one, so neither lint contour builds it\n", $1, $2 > "/dev/stderr"
				refused = 1
			}
		}
		END {
			for (t in found) {
				print t
			}
			exit refused
		}' |
		sort
)"

if [[ $joined -eq 1 ]]; then
	printf '%s\n' "$tags" | paste -sd ',' -
else
	printf '%s\n' "$tags"
fi

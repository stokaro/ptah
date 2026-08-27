#!/usr/bin/env bash
# Requires every goreleaser artifact name to be a file name rather than a path.
#
# goreleaser renders artifact names from templates and writes the result
# straight into dist/. A template that interpolates something carrying a slash
# therefore asks for a subdirectory nothing created, and the build dies at
# whichever artifact is written first with "could not open ... for writing: No
# such file or directory" -- a message that names a missing directory and says
# nothing about the tag that produced it.
#
# The value that carries a slash is the version. Left to itself goreleaser
# derives a snapshot's version from `git describe`, which answers with the
# nearest reachable tag, and a tag may be named anything at all: this repository
# has `prototype/canonical-core`, pushed as a marker, and from the moment it
# existed every branch's release job failed.
#
# So the rule is that the snapshot version is pinned to something that cannot
# contain a separator, and this gate reads the config rather than waiting eleven
# minutes for a build to discover it again.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
config="$repo_root/.goreleaser.yaml"

if [ ! -f "$config" ]; then
	echo "check-goreleaser-artifact-names: $config is missing" >&2
	exit 1
fi

template="$(awk '
	/^snapshot:/ { in_snapshot = 1; next }
	/^[^[:space:]#]/ { in_snapshot = 0 }
	in_snapshot && /version_template:/ {
		sub(/^[[:space:]]*version_template:[[:space:]]*/, "")
		gsub(/^["'"'"']|["'"'"']$/, "")
		print
		exit
	}
' "$config")"

if [ -z "$template" ]; then
	echo "check-goreleaser-artifact-names: snapshot.version_template is not set in .goreleaser.yaml" >&2
	echo "  Without it goreleaser derives the snapshot version from the nearest git tag, and a tag" >&2
	echo "  containing a slash puts a path separator into every artifact name." >&2
	exit 1
fi

# The three template values that can carry whatever a person typed into a ref.
for field in .Version .Tag .Branch .Summary; do
	if printf '%s' "$template" | grep -qF -- "$field"; then
		echo "check-goreleaser-artifact-names: snapshot.version_template reads $field" >&2
		echo "  $template" >&2
		echo "  That value comes from a git ref, and a ref may contain a slash. Use .ShortCommit," >&2
		echo "  which identifies a snapshot and cannot name a directory." >&2
		exit 1
	fi
done

if printf '%s' "$template" | grep -q '/'; then
	echo "check-goreleaser-artifact-names: snapshot.version_template contains a literal slash" >&2
	echo "  $template" >&2
	exit 1
fi

echo "goreleaser artifact names: OK (snapshot version is ${template})"

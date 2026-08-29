#!/usr/bin/env bash
# The Go toolchain must be declared exactly once, as the `toolchain` directive
# in the root go.mod, and every other place that needs it must derive it.
#
# The failure this exists to stop: a dependency bump titled "update dependency
# golang to v1.26.6" merged having changed one file, .golangci.yml, because that
# was the only file its updater could reach. The literal still stood in eighteen
# setup-go steps and an action input default, so CI kept building with the older
# toolchain and govulncheck reported seven standard-library vulnerabilities
# against it. Nothing failed while the declarations disagreed.
#
# The rules are internal/gotoolchain, tested against fixtures rather than only
# against this repository. 642 lines of shell stood here and hand-rolled a
# partial YAML parser -- step extraction by indentation, quoted keys, scalar
# quote stripping, block-scalar refusal, and a small parser for two GitHub
# expression shapes -- which grew a detector every time YAML permitted an
# equivalent spelling the previous scan had not anticipated (stokaro/ptah#2511).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
exec go run ./internal/cmd/gotoolchain "$@"

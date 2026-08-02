package main

import (
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/cmd/root"
)

func main() {
	// The Atlas-compatible surface is quiet by construction: Atlas CE writes
	// nothing to stderr for a clean run of the commands this binary mirrors,
	// so library narration must not reach this process's streams either.
	// Without this, a `--format` report piped through `2>&1 | jq` stops being a
	// single JSON document (stokaro/ptah#967). Quiet is not silent — log-only
	// Warn and Error diagnostics still get through.
	cliobs.QuietDefaultLogger()
	root.ExecuteCommand(atlas.NewCompatCommand(filepath.Base(os.Args[0])))
}

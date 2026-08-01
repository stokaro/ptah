package main

import (
	"os"
	"path/filepath"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/internal/cliobs"
	"github.com/stokaro/ptah/cmd/root"
)

func main() {
	// The Atlas-compatible surface is quiet by construction: Atlas CE writes
	// nothing to stderr for the commands this binary mirrors, so no library
	// logging may reach this process's streams either. Without this, a
	// `--format` report piped through `2>&1 | jq` stops being a single JSON
	// document (stokaro/ptah#967).
	cliobs.SilenceDefaultLogger()
	root.ExecuteCommand(atlas.NewCompatCommand(filepath.Base(os.Args[0])))
}

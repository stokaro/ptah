package main

import (
	"fmt"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
)

func main() {
	policy, err := atlascompatpolicy.Resolve()
	if err == nil {
		err = atlas.ValidateStrictCompatFlagEnvironment(policy)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1) //revive:disable-line:deep-exit // main owns the process-level compatibility contract.
	}
	// The Atlas-compatible surface is quiet by construction: Atlas CE writes
	// nothing to stderr for a clean run of the commands this binary mirrors,
	// so library narration must not reach this process's streams either.
	// Without this, a `--format` report piped through `2>&1 | jq` stops being a
	// single JSON document (stokaro/ptah#967). Quiet is not silent — log-only
	// Warn and Error diagnostics still get through.
	cliobs.QuietDefaultLogger()
	root.ExecuteCommand(atlas.NewCompatCommandWithPolicy(filepath.Base(os.Args[0]), policy))
}

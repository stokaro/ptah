package main

import (
	"fmt"
	"os"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/exeext"
)

func main() {
	policy, err := atlascompatpolicy.Resolve()
	if err == nil {
		err = atlas.ValidateStrictCompatFlagEnvironment(policy)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1) //revive:disable-line:deep-exit main owns the process-level compatibility contract
	}
	// The Atlas-compatible surface is quiet by construction: Atlas CE writes
	// nothing to stderr for a clean run of the commands this binary mirrors,
	// so library narration must not reach this process's streams either.
	// Without this, a `--format` report piped through `2>&1 | jq` stops being a
	// single JSON document (stokaro/ptah#967). Quiet is not silent — log-only
	// Warn and Error diagnostics still get through.
	cliobs.QuietDefaultLogger()
	// The displayed name drops the platform's executable extension. Measured
	// against the pinned community binary: a copy of it renamed to
	// atlas-renamed.exe still prints `Usage:\n  atlas migrate [command]`, so its
	// name is static and a drop-in installed as atlas.exe -- which is the only
	// way to install one on Windows -- has to say atlas too.
	root.ExecuteCommand(atlas.NewCompatCommandWithPolicy(exeext.TrimmedBase(os.Args[0]), policy))
}

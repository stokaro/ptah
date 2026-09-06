// Command docsync keeps every generated documentation block equal to what its
// generator renders, and rewrites them all on `--write`.
//
// It replaces five shell wrappers that each carried the same engine: argument
// parsing, marker extraction, carrier discovery, diff rendering, and five
// byte-identical copies of an embedded Python program. The engine is
// internal/docsync; this file is the declaration of what there is to keep in
// step (stokaro/ptah#2510).
//
// The generators run IN THIS PROCESS. Every one of them is already a library
// -- capabilityprobe, lintcatalog, agentsurface and cmdrefviews -- so the
// eleven `go run` starts the wrappers made are one build and one run.
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"ptah.run/cmd/root"
	"ptah.run/internal/agentsurface"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/cmdrefviews"
	"ptah.run/internal/docsync"
	"ptah.run/internal/lintcatalog"
	"ptah.run/internal/schemacensus"
)

const usage = `usage: docsync [--write]

Checks every generated documentation block against its generator. --write
rewrites them instead.
`

// targets is the whole declaration. A new generated surface is an entry here,
// not another script.
func targets() []docsync.Target {
	return []docsync.Target{
		{
			Name: "the release-line matrix", Path: "docs/capabilities.md",
			Begin: "<!-- BEGIN GENERATED VERSION MATRIX -->", End: "<!-- END GENERATED VERSION MATRIX -->",
			Source: "internal/capabilityprobe/cells.go",
			Render: writerOf(capabilityprobe.WriteMatrixMarkdown),
		},
		{
			// The site's responsive check refuses a table wider than its
			// reading column, so the site carries a narrow view of the same
			// cells. Two renderings, one declaration.
			Name: "the release-line matrix, narrow", Path: "docs/site/src/content/docs/databases/support-matrix.md",
			Begin: "<!-- BEGIN GENERATED VERSION MATRIX -->", End: "<!-- END GENERATED VERSION MATRIX -->",
			Source: "internal/capabilityprobe/cells.go",
			Render: writerOf(capabilityprobe.WriteMatrixSummary),
		},
		{
			Name: "the capability keys", Path: "docs/capabilities.md",
			Begin: "<!-- BEGIN GENERATED CAPABILITY KEYS -->", End: "<!-- END GENERATED CAPABILITY KEYS -->",
			Source: "the capability registry",
			Render: writerOf(capabilityprobe.WriteCapabilityKeyMarkdown),
		},
		{
			Name: "the preset matrix", Path: "docs/capabilities.md",
			Begin: "<!-- BEGIN GENERATED PRESET MATRIX -->", End: "<!-- END GENERATED PRESET MATRIX -->",
			Source: "capability.NamedPresets",
			Render: writerOf(capabilityprobe.WritePresetMarkdown),
		},
		{
			Name: "the lint rules", Path: "docs/site/src/content/docs/reference/lint-rules.md",
			Begin: "<!-- BEGIN GENERATED LINT RULES -->", End: "<!-- END GENERATED LINT RULES -->",
			Source: "internal/lintcatalog",
			Render: lintcatalog.WriteMarkdown,
		},
		{
			Name: "the field dispositions", Path: "docs/schema_field_dispositions.md",
			Begin: "<!-- BEGIN GENERATED FIELD DISPOSITIONS -->", End: "<!-- END GENERATED FIELD DISPOSITIONS -->",
			Source: "internal/schemacensus",
			Render: schemacensus.WriteMarkdown,
		},
		{
			Name: "the agent surface", Path: "docs/agent-surface.md",
			Begin: "<!-- BEGIN GENERATED AGENT SURFACE -->", End: "<!-- END GENERATED AGENT SURFACE -->",
			Source: "internal/agentsurface",
			Render: agentSurfaceMarkdown,
		},
		{
			Name: "the database-safe verbs", Path: "docs/agent-surface.md",
			Begin: "<!-- BEGIN GENERATED DATABASE-SAFE VERBS -->", End: "<!-- END GENERATED DATABASE-SAFE VERBS -->",
			Source: "internal/agentsurface",
			Render: databaseSafeMarkdown,
		},
		{
			Name: "the native command table", Path: "docs/site/src/content/docs/reference/native-commands.md",
			Begin: "<!-- BEGIN GENERATED NATIVE COMMANDS -->", End: "<!-- END GENERATED NATIVE COMMANDS -->",
			Source: "the ptah command tree",
			Render: view("native"),
		},
		{
			Name: "the compat command table", Path: "docs/site/src/content/docs/reference/atlas-commands.md",
			Begin: "<!-- BEGIN GENERATED COMPAT COMMANDS -->", End: "<!-- END GENERATED COMPAT COMMANDS -->",
			Source: "the ptah-compat command tree",
			Render: view("compat"),
		},
		{
			Name: "the strict-compat classification", Path: "docs/site/src/content/docs/reference/atlas-commands.md",
			Begin: "<!-- BEGIN GENERATED STRICT COMPAT CLASSIFICATION -->", End: "<!-- END GENERATED STRICT COMPAT CLASSIFICATION -->",
			Source: "PTAH_ATLAS_STRICT_COMPAT=1",
			Render: view("strict"),
		},
		{
			// A whole generated page: no markers, because there is no
			// hand-written half to keep.
			Name: "the flag reference", Path: "docs/site/src/content/docs/reference/command-flags.md",
			Source: "both command trees",
			Render: view("flags"),
		},
	}
}

// writerOf adapts a generator that cannot fail.
func writerOf(write func(io.Writer)) func(io.Writer) error {
	return func(w io.Writer) error {
		write(w)
		return nil
	}
}

// agentSurfaceMarkdown renders the whole verb table.
func agentSurfaceMarkdown(w io.Writer) error {
	return writeAgentSurface(w, agentsurface.Markdown)
}

// databaseSafeMarkdown renders the verbs that cannot change a database.
func databaseSafeMarkdown(w io.Writer) error {
	return writeAgentSurface(w, agentsurface.DatabaseSafeMarkdown)
}

// writeAgentSurface walks the tree once and refuses an empty walk here rather
// than letting the empty-output rule report it: "the command tree has no
// runnable verbs" says what is wrong, and "the generator produced nothing" says
// only that something is.
func writeAgentSurface(w io.Writer, render func([]agentsurface.Leaf) string) error {
	leaves := agentsurface.Walk(root.NewRootCommand())
	if len(leaves) == 0 {
		return fmt.Errorf("the command tree has no runnable verbs")
	}
	_, err := io.WriteString(w, render(leaves))
	return err
}

// view renders one command-reference view.
func view(name string) func(io.Writer) error {
	return func(w io.Writer) error {
		rendered, err := cmdrefviews.Render(name)
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, rendered)
		return err
	}
}

func main() {
	write := false
	switch {
	case len(os.Args) == 1:
	case len(os.Args) == 2 && os.Args[1] == "--write":
		write = true
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	repoRoot, err := repositoryRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "docsync:", err)
		os.Exit(1)
	}

	all := targets()
	if len(all) == 0 {
		fmt.Fprintln(os.Stderr, "docsync: no targets declared; refusing to report a pass")
		os.Exit(1)
	}

	if write {
		os.Exit(rewrite(repoRoot, all))
	}
	os.Exit(check(repoRoot, all))
}

func rewrite(repoRoot string, all []docsync.Target) int {
	status := 0
	for _, target := range all {
		changed, err := docsync.Write(repoRoot, target)
		if err != nil {
			fmt.Fprintln(os.Stderr, "docsync:", err)
			status = 1
			continue
		}
		if changed {
			fmt.Printf("docsync: rewrote %s in %s\n", target.Name, target.Path)
		}
	}
	return status
}

func check(repoRoot string, all []docsync.Target) int {
	status := 0
	for _, target := range all {
		result := docsync.Check(repoRoot, target)
		switch {
		case result.Problem != "":
			fmt.Fprintln(os.Stderr, "docsync:", result.Problem)
			status = 1
		case result.Stale:
			fmt.Fprintf(os.Stderr, "docsync: %s in %s is out of date with %s\n",
				target.Name, target.Path, target.Source)
			fmt.Fprint(os.Stderr, result.Diff)
			status = 1
		}
	}

	problems, err := docsync.CheckCarriers(repoRoot, all)
	if err != nil {
		fmt.Fprintln(os.Stderr, "docsync:", err)
		return 1
	}
	for _, problem := range problems {
		fmt.Fprintln(os.Stderr, "docsync:", problem)
		status = 1
	}

	if status == 0 {
		fmt.Printf("docsync: OK (%d generated blocks match their generators)\n", len(all))
	} else {
		fmt.Fprintln(os.Stderr, "docsync: run `go run ./internal/cmd/docsync --write`")
	}
	return status
}

// repositoryRoot asks git, so the command works from any directory.
func repositoryRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

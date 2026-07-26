// Package schemaload resolves a desired-state schema from Go entity roots and/or
// language-agnostic schema files, merging multiple sources into one composite
// schema. It is the shared desired-source resolver behind the render, compare,
// and migrate commands, so every command accepts the same --root-dir and
// --schema-file inputs and composes them the same way.
package schemaload

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/schemafile"
)

// Options selects the desired-schema sources and how loading is reported.
type Options struct {
	// RootDirs are Go entity roots scanned for migrator directives (repeatable).
	RootDirs []string
	// SchemaFiles are YAML, HCL, or SQL schema files (repeatable).
	SchemaFiles []string
	// Dialect is an optional dialect hint used when parsing SQL schema files.
	Dialect string
	// Logf, when non-nil, receives human-readable progress messages. Commands
	// that emit machine-readable output (SQL, safety reports) leave it nil so the
	// resolver stays quiet.
	Logf func(format string, args ...any)
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// Sources returns a human-readable, comma-separated list of the configured
// desired-schema sources, applying the same current-directory default that Load
// applies when nothing is configured. It is intended for progress and report
// headers.
func (o Options) Sources() string {
	parts := make([]string, 0, len(o.RootDirs)+len(o.SchemaFiles))
	parts = append(parts, o.RootDirs...)
	parts = append(parts, o.SchemaFiles...)
	if len(parts) == 0 {
		parts = append(parts, "./")
	}
	return strings.Join(parts, ", ")
}

// Load resolves the desired schema described by opts. With no source at all it
// defaults to scanning the current directory for Go entities (the historical
// behavior). Multiple sources of any kind are merged into one composite schema.
func Load(opts Options) (*goschema.Database, error) {
	rootDirs := opts.RootDirs
	schemaFiles := opts.SchemaFiles

	// With no source of any kind, default to scanning the current directory for
	// Go entities.
	if len(rootDirs) == 0 && len(schemaFiles) == 0 {
		rootDirs = []string{"./"}
	}

	// Single-source fast paths: Go roots only, or exactly one schema file.
	if len(schemaFiles) == 0 {
		return opts.loadGoRoots(rootDirs)
	}
	if len(rootDirs) == 0 && len(schemaFiles) == 1 {
		return opts.loadSchemaFile(schemaFiles[0])
	}

	// Composite: merge the Go roots (parsed un-finalized so Merge runs a single
	// finalize pass) with each schema file.
	var sources []*goschema.Database
	if len(rootDirs) > 0 {
		absRoots, err := resolveRootDirs(rootDirs)
		if err != nil {
			return nil, err
		}
		for _, absPath := range absRoots {
			opts.logf("Scanning directory: %s", absPath)
		}
		goDB, err := goschema.ParseDirRaw(absRoots...)
		if err != nil {
			return nil, fmt.Errorf("error parsing packages: %w", err)
		}
		sources = append(sources, goDB)
	}
	for _, schemaFile := range schemaFiles {
		fileDB, err := opts.loadSchemaFile(schemaFile)
		if err != nil {
			return nil, err
		}
		sources = append(sources, fileDB)
	}

	result, err := goschema.Merge(sources...)
	if err != nil {
		return nil, fmt.Errorf("error merging composite schema: %w", err)
	}
	return result, nil
}

// loadGoRoots parses one or more Go entity roots into a finalized composite
// schema.
func (o Options) loadGoRoots(rootDirs []string) (*goschema.Database, error) {
	absRoots, err := resolveRootDirs(rootDirs)
	if err != nil {
		return nil, err
	}

	for _, absPath := range absRoots {
		o.logf("Scanning directory: %s", absPath)
	}

	result, err := goschema.ParseDirs(absRoots...)
	if err != nil {
		return nil, fmt.Errorf("error parsing packages: %w", err)
	}
	return result, nil
}

// resolveRootDirs turns each root into an absolute path and fails fast if any
// does not exist.
func resolveRootDirs(rootDirs []string) ([]string, error) {
	absRoots := make([]string, 0, len(rootDirs))
	for _, rootDir := range rootDirs {
		absPath, err := filepath.Abs(rootDir)
		if err != nil {
			return nil, fmt.Errorf("error resolving path: %w", err)
		}
		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("directory does not exist: %s", absPath)
		}
		absRoots = append(absRoots, absPath)
	}
	return absRoots, nil
}

// loadSchemaFile resolves a single YAML, HCL, or SQL schema file into a
// finalized schema.
func (o Options) loadSchemaFile(schemaFile string) (*goschema.Database, error) {
	absPath, err := filepath.Abs(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("error resolving schema file: %w", err)
	}

	// Reject unsupported extensions here so the error is reported without the
	// generic "error parsing schema file" wrapper, matching the render command's
	// long-standing message.
	switch strings.ToLower(filepath.Ext(absPath)) {
	case ".yaml", ".yml", ".hcl", ".sql":
	default:
		return nil, fmt.Errorf("unsupported schema file extension %q: only .yaml, .yml, .hcl, and .sql are supported", filepath.Ext(absPath))
	}

	o.logf("Reading schema file: %s", absPath)

	result, err := schemafile.LoadPath(absPath, schemafile.Options{Dialect: o.Dialect})
	if err != nil {
		return nil, fmt.Errorf("error parsing schema file: %w", err)
	}
	return result, nil
}

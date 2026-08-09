// Package schemaload resolves a desired-state schema from Go entity roots and/or
// language-agnostic schema files, merging multiple sources into one composite
// schema. It is the shared desired-source resolver behind the render, compare,
// and migrate commands, so every command accepts the same --root-dir and
// --schema-file inputs and composes them the same way.
package schemaload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/schemaartifact"
	"go.5x5.cz/ptah/internal/schemafile"
)

// Options selects the desired-schema sources and how loading is reported.
type Options struct {
	// RootDirs are Go entity roots scanned for migrator directives (repeatable).
	RootDirs []string
	// SchemaFiles are YAML, HCL, or SQL schema files (repeatable).
	SchemaFiles []string
	// Commands are external programs whose stdout is a desired schema
	// (repeatable). Each runs directly without a shell.
	Commands []schemasource.Command
	// Dialect is an optional dialect hint used when parsing SQL schema files.
	Dialect string
	// PlainHTTP explicitly permits an unencrypted local OCI registry.
	PlainHTTP bool
	// Logf, when non-nil, receives human-readable progress messages. Commands
	// that emit machine-readable output (SQL, safety reports) leave it nil so the
	// resolver stays quiet.
	Logf func(format string, args ...any)
}

// OCI identifies the immutable registry artifact behind a desired schema.
type OCI struct {
	Client     *ociartifact.Client
	Reference  string
	Descriptor ocispec.Descriptor
}

// Result is a resolved desired schema with optional immutable OCI provenance.
// OCI is populated only when the desired state came from exactly one OCI
// schema artifact, so callers cannot attach metadata to an ambiguous subject.
type Result struct {
	Database *goschema.Database
	OCI      *OCI
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
	parts := make([]string, 0, len(o.RootDirs)+len(o.SchemaFiles)+len(o.Commands))
	parts = append(parts, o.RootDirs...)
	parts = append(parts, o.SchemaFiles...)
	for _, command := range o.Commands {
		parts = append(parts, commandDisplay(command))
	}
	if len(parts) == 0 {
		parts = append(parts, "./")
	}
	return strings.Join(parts, ", ")
}

// Load resolves the desired schema described by opts using a background context.
// It is a convenience wrapper over LoadContext for callers that do not run
// external schema commands or do not need cancellation.
func Load(opts Options) (*goschema.Database, error) {
	return LoadContext(context.Background(), opts)
}

// LoadContext resolves the desired schema described by opts. With no source at
// all it defaults to scanning the current directory for Go entities (the
// historical behavior). Multiple sources of any kind are merged into one
// composite schema. Any external schema commands are run under ctx.
func LoadContext(ctx context.Context, opts Options) (*goschema.Database, error) {
	result, err := LoadResult(ctx, opts)
	if err != nil {
		return nil, err
	}
	return result.Database, nil
}

// LoadResult resolves the desired schema and preserves immutable OCI
// provenance when the input is exactly one OCI schema artifact.
func LoadResult(ctx context.Context, opts Options) (*Result, error) {
	if raw, ok := singleOCIReference(opts); ok {
		return opts.loadOCIResult(ctx, raw)
	}
	database, err := loadContext(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Result{Database: database}, nil
}

func loadContext(ctx context.Context, opts Options) (*goschema.Database, error) {
	rootDirs := opts.RootDirs
	schemaFiles := opts.SchemaFiles
	commands := opts.Commands

	// With no source of any kind, default to scanning the current directory for
	// Go entities.
	if len(rootDirs) == 0 && len(schemaFiles) == 0 && len(commands) == 0 {
		rootDirs = []string{"./"}
	}

	// Single-source fast paths: Go roots only, exactly one schema file, or
	// exactly one command.
	if len(schemaFiles) == 0 && len(commands) == 0 {
		return opts.loadGoRoots(rootDirs)
	}
	if len(rootDirs) == 0 && len(commands) == 0 && len(schemaFiles) == 1 {
		return opts.loadSchemaFile(ctx, schemaFiles[0])
	}
	if len(rootDirs) == 0 && len(schemaFiles) == 0 && len(commands) == 1 {
		return opts.loadCommand(ctx, commands[0])
	}

	// Composite: merge the Go roots (parsed un-finalized so Merge runs a single
	// finalize pass) with each schema file and command output.
	var sources []*goschema.Database
	if len(rootDirs) > 0 {
		absRoots, err := resolveRootDirs(rootDirs)
		if err != nil {
			return nil, err
		}
		for _, absPath := range absRoots {
			opts.logf("Scanning directory: %s", absPath)
			goDB, err := goschema.ParseDirRaw(absPath)
			if err != nil {
				return nil, fmt.Errorf("error parsing packages: %w", err)
			}
			// Preserve each root as one source so Merge can distinguish an
			// internal cross-file duplicate from a cross-source conflict.
			sources = append(sources, goDB)
		}
	}
	for _, schemaFile := range schemaFiles {
		fileDB, err := opts.loadSchemaFile(ctx, schemaFile)
		if err != nil {
			return nil, err
		}
		sources = append(sources, fileDB)
	}
	for _, command := range commands {
		commandDB, err := opts.loadCommand(ctx, command)
		if err != nil {
			return nil, err
		}
		sources = append(sources, commandDB)
	}

	result, err := goschema.Merge(sources...)
	if err != nil {
		return nil, fmt.Errorf("error merging composite schema: %w", err)
	}
	return result, nil
}

func singleOCIReference(opts Options) (string, bool) {
	if len(opts.RootDirs) != 0 || len(opts.Commands) != 0 || len(opts.SchemaFiles) != 1 {
		return "", false
	}
	raw := opts.SchemaFiles[0]
	return raw, strings.HasPrefix(raw, ociartifact.Scheme)
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

// loadCommand runs an external schema command and returns its parsed output. The
// resolver's dialect hint is applied when the command does not set its own.
func (o Options) loadCommand(ctx context.Context, command schemasource.Command) (*goschema.Database, error) {
	if command.Dialect == "" {
		command.Dialect = o.Dialect
	}
	o.logf("Running schema command: %s", commandDisplay(command))
	return schemasource.Run(ctx, command)
}

func commandDisplay(command schemasource.Command) string {
	if len(command.Args) == 0 {
		return "external command"
	}
	name := filepath.Base(command.Args[0])
	if len(command.Args) == 1 {
		return "$(" + name + ")"
	}
	return fmt.Sprintf("$(%s + %d args)", name, len(command.Args)-1)
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

// loadSchemaFile resolves a single YAML, HCL, or SQL schema file — or a
// directory of .sql or .hcl schema files — into a finalized schema.
func (o Options) loadSchemaFile(ctx context.Context, schemaFile string) (*goschema.Database, error) {
	if strings.HasPrefix(schemaFile, ociartifact.Scheme) {
		return o.loadOCI(ctx, schemaFile)
	}
	if err := rejectEnvReference(schemaFile); err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("error resolving schema file: %w", err)
	}

	// Reject unsupported extensions here so the error is reported without the
	// generic "error parsing schema file" wrapper, matching the render command's
	// long-standing message.
	//
	// A directory has no extension to check, and the extension rule used to
	// refuse it with `unsupported schema file extension ""` — a message about a
	// file for something that is not one. The loader decides what a directory
	// of schema files means, so a directory skips this switch and reaches it.
	if !isSchemaDir(absPath) {
		switch strings.ToLower(filepath.Ext(absPath)) {
		case ".yaml", ".yml", ".hcl", ".sql":
		default:
			return nil, fmt.Errorf("unsupported schema file extension %q: only .yaml, .yml, .hcl, and .sql are supported", filepath.Ext(absPath))
		}
	}

	o.logf("Reading schema file: %s", absPath)

	result, err := schemafile.LoadPath(absPath, schemafile.Options{Dialect: o.Dialect})
	if err != nil {
		return nil, fmt.Errorf("error parsing schema file: %w", err)
	}
	return result, nil
}

// isSchemaDir reports whether path names an existing directory. A path that
// cannot be stat'ed is not one: the loader reports a missing path with a better
// message than the extension switch would.
func isSchemaDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// envScheme is the desired-state reference scheme that reads an attribute out
// of the selected atlas.hcl env.
const envScheme = "env://"

// rejectEnvReference refuses an env:// --schema-file value with a diagnostic
// that names env://.
//
// The native binary resolves no env:// reference: it has no route from the
// selected atlas.hcl env to a desired-state source, so a value that reached
// [filepath.Abs] was treated as a relative path and failed on its (empty)
// extension. That message named neither env:// nor the attribute, and was
// byte-identical for a valid attribute and a misspelled one.
//
// The two cases are separated here because they need different actions: a
// misspelled attribute is fixed by spelling it correctly, while a correctly
// spelled one is fixed by naming the file or by switching binaries. The
// attribute vocabulary comes from atlassource so the two surfaces cannot
// advertise different lists.
func rejectEnvReference(schemaFile string) error {
	trimmed := strings.TrimSpace(schemaFile)
	if !strings.HasPrefix(strings.ToLower(trimmed), envScheme) {
		return nil
	}
	source, err := atlassource.Classify(trimmed)
	if err != nil {
		return fmt.Errorf("--schema-file %q: %w", schemaFile, err)
	}
	if err := atlassource.ValidateEnvAttr(source.EnvAttr); err != nil {
		return fmt.Errorf("--schema-file %q: %w", schemaFile, err)
	}
	return fmt.Errorf(
		"--schema-file %q: %s references are not resolved by ptah; pass the schema file itself, "+
			"or use ptah-compat, whose --to and --from accept %s%s",
		schemaFile,
		envScheme,
		envScheme,
		source.EnvAttr,
	)
}

func (o Options) loadOCI(ctx context.Context, raw string) (*goschema.Database, error) {
	result, err := o.loadOCIResult(ctx, raw)
	if err != nil {
		return nil, err
	}
	return result.Database, nil
}

func (o Options) loadOCIResult(ctx context.Context, raw string) (*Result, error) {
	ref, err := ociartifact.ParseRef(raw)
	if err != nil {
		return nil, err
	}
	o.logf("Pulling schema artifact: %s", ref)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: o.PlainHTTP})
	if err != nil {
		return nil, err
	}
	artifact, err := schemaartifact.Pull(ctx, client, ref.String())
	if err != nil {
		return nil, fmt.Errorf("resolve schema artifact: %w", err)
	}
	return &Result{
		Database: artifact.Database,
		OCI: &OCI{
			Client:     client,
			Reference:  ref.String(),
			Descriptor: artifact.Descriptor,
		},
	}, nil
}

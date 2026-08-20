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
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/core/coverage"
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
	// ProjectEnv is the evaluated atlas.hcl environment an `env://` schema file
	// is expanded through. The zero value keeps the refusal
	// [rejectEnvReference] writes, which is what a run that selected no project
	// configuration should get: there is nothing to resolve the reference
	// against (stokaro/ptah#1760).
	ProjectEnv atlassource.ProjectEnv
	// EnvSelectorFlag names the flag that selects a project environment on the
	// running command, or is empty when the command offers none. It only
	// shapes the refusal: a command with a selector can resolve the reference
	// on the next run, and a command without one never can.
	EnvSelectorFlag string
	// Commands are external programs whose stdout is a desired schema
	// (repeatable). Each runs directly without a shell.
	Commands []schemasource.Command
	// Dialect is an optional dialect hint used when parsing SQL schema files.
	Dialect string
	// Vars supplies values for the `variable` blocks of an HCL schema file, in
	// the `name=value` spelling `--var` takes. See [schemafile.Options.Vars].
	//
	// Without it every verb resolving its desired state through this loader --
	// `schema apply`, `schema plan`, `compare`, `generate`, `migrate generate`,
	// `schema export` and `schema test` -- read an HCL schema with its declared
	// defaults whatever the caller passed, and refused a file whose variable
	// had none (stokaro/ptah#1533).
	Vars []string
	// VarValues supplies already-decoded values, which is the form an
	// atlas.hcl data source scope resolves to. It is carried beside Vars
	// rather than folded into it because a value containing `=` survives a map
	// and does not survive a round trip through the flag spelling. See
	// [schemafile.Options.VarValues].
	VarValues map[string]string
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
	schemaFiles, commands, err := opts.expandEnvSchemaFiles(opts.SchemaFiles, opts.Commands)
	if err != nil {
		return nil, err
	}

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

	return opts.loadCompositeContext(ctx, rootDirs, schemaFiles, commands)
}

func (o Options) loadCompositeContext(
	ctx context.Context,
	rootDirs, schemaFiles []string,
	commands []schemasource.Command,
) (*goschema.Database, error) {
	// Parse Go roots un-finalized so Merge runs a single finalize pass.
	var sources []*goschema.Database
	if len(rootDirs) > 0 {
		absRoots, err := resolveRootDirs(rootDirs)
		if err != nil {
			return nil, err
		}
		for _, absPath := range absRoots {
			o.logf("Scanning directory: %s", absPath)
			goDB, err := goschema.ParseDirRaw(absPath)
			if err != nil {
				return nil, fmt.Errorf("error parsing packages: %w", err)
			}
			goDB = withGoAnnotationLimits(goDB)
			// Preserve each root as one source so Merge can distinguish an
			// internal cross-file duplicate from a cross-source conflict.
			sources = append(sources, goDB)
		}
	}
	for _, schemaFile := range schemaFiles {
		fileDB, err := o.loadSchemaFile(ctx, schemaFile)
		if err != nil {
			return nil, err
		}
		sources = append(sources, fileDB)
	}
	for _, command := range commands {
		commandDB, err := o.loadCommand(ctx, command)
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
	return withGoAnnotationLimits(result), nil
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

// expandEnvSchemaFiles turns each `env://` schema file into the sources the
// selected atlas.hcl environment names, and leaves every other value alone.
//
// It runs before the single-source fast paths below so an env reference that
// names several files merges through the same route a repeated --schema-file
// takes, rather than needing a second merge of its own.
//
// Without a loaded project configuration the reference is refused exactly as
// before. That is not a fallback: there is nothing to resolve it against, and
// the refusal names the attribute and the alternative.
func (o Options) expandEnvSchemaFiles(
	schemaFiles []string,
	commands []schemasource.Command,
) ([]string, []schemasource.Command, error) {
	if !containsEnvReference(schemaFiles) {
		return schemaFiles, commands, nil
	}
	expandedFiles := make([]string, 0, len(schemaFiles))
	expandedCommands := commands
	for _, schemaFile := range schemaFiles {
		if !isEnvReference(schemaFile) {
			expandedFiles = append(expandedFiles, schemaFile)
			continue
		}
		if !o.ProjectEnv.Loaded {
			return nil, nil, rejectEnvReference(schemaFile, o.EnvSelectorFlag)
		}
		set, err := atlassource.ClassifySet("--schema-file", []string{schemaFile}, o.ProjectEnv)
		if err != nil {
			return nil, nil, fmt.Errorf("--schema-file %q: %w", schemaFile, err)
		}
		for _, source := range set.Sources {
			switch source.Kind {
			case atlassource.KindLocalFile:
				expandedFiles = append(expandedFiles, localFilePath(source))
			case atlassource.KindExternalSchema:
				expandedCommands = append(expandedCommands, source.Command)
			default:
				return nil, nil, fmt.Errorf(
					"--schema-file %q: the selected environment names a %s (%s), which --schema-file does not read; "+
						"pass a schema file, or use ptah-compat, whose --to and --from read it",
					schemaFile, source.Kind, source.Raw,
				)
			}
		}
	}
	return expandedFiles, expandedCommands, nil
}

// localFilePath prefers the path ClassifySet resolved, because the raw value
// still carries the file:// scheme the schema reader would treat as a
// directory component.
func localFilePath(source atlassource.Source) string {
	if source.Path != "" {
		return source.Path
	}
	return source.Raw
}

func containsEnvReference(schemaFiles []string) bool {
	return slices.ContainsFunc(schemaFiles, isEnvReference)
}

func isEnvReference(schemaFile string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(schemaFile)), envScheme)
}

// loadSchemaFile resolves a single YAML, HCL, or SQL schema file — or a
// directory of .sql or .hcl schema files — into a finalized schema.
func (o Options) loadSchemaFile(ctx context.Context, schemaFile string) (*goschema.Database, error) {
	if strings.HasPrefix(schemaFile, ociartifact.Scheme) {
		return o.loadOCI(ctx, schemaFile)
	}
	if err := rejectEnvReference(schemaFile, o.EnvSelectorFlag); err != nil {
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

	// The operator's own spelling reaches the loader, not absPath.
	//
	// schemafile resolves the path through pathguard.ResolveCLIPath, which
	// confines a relative path to the working directory and exempts an absolute
	// one. Handing it absPath therefore spelled every path the exempt way and
	// switched the guard off for this whole surface: `--schema-file
	// ../outside.sql` loaded, while ptah-compat refused that identical
	// destination through that identical guard. absPath is a display and
	// extension-check convenience above; it must not become the value the guard
	// judges.
	result, err := schemafile.LoadPath(schemaFile, schemafile.Options{
		Dialect:   o.Dialect,
		Vars:      o.Vars,
		VarValues: o.VarValues,
	})
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
func rejectEnvReference(schemaFile, envSelectorFlag string) error {
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
	if envSelectorFlag != "" {
		return fmt.Errorf(
			"--schema-file %q: %s names the %q attribute of a selected project environment, and this run selected none; "+
				"pass --%s, or pass the schema file itself",
			schemaFile,
			envScheme,
			source.EnvAttr,
			envSelectorFlag,
		)
	}
	return fmt.Errorf(
		"--schema-file %q: %s names the %q attribute of a project environment, and this command selects none; "+
			"pass the schema file itself, or use a command that takes --env, or ptah-compat, whose --to and --from accept %s%s",
		schemaFile,
		envScheme,
		source.EnvAttr,
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

// withGoAnnotationLimits records what the Go annotation language cannot
// express, so a comparator reads its silence about such an object as "outside
// this source's managed surface" rather than as a removal.
//
// There is no //ptah:schema: directive for a SQLite virtual table, so a Go
// schema in front of a live FTS5 index has not withheld one. Dropping it takes
// the index and everything in it, and no annotation could have asked to keep it
// (stokaro/ptah#1028).
//
// It is recorded here rather than in the parser because the parser answers what
// a SOURCE declares and its answer is rendered back out; a limit invented
// during parsing would surface as a directive nobody wrote.
func withGoAnnotationLimits(database *goschema.Database) *goschema.Database {
	if database == nil {
		return nil
	}
	database.NotDescribed = database.NotDescribed.WithKind(coverage.VirtualTable)
	return database
}

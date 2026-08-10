package schema

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemaload"
)

// loadExportSchema resolves the desired schema an export projects, from Go
// annotation roots and/or language-agnostic schema files.
//
// It delegates to schemaload, the resolver behind "ptah schema render",
// "ptah schema compare" and the migration commands, so --schema-file means the
// same thing on every surface that accepts it. Parsing a schema file here
// instead would be a second loader free to drift from the first.
func loadExportSchema(cmd *cobra.Command, opts exportOptions) (*goschema.Database, error) {
	rootDirs, err := exportRootDirs(opts)
	if err != nil {
		return nil, err
	}
	// Logf stays nil so the resolver narrates nothing: with --out omitted the
	// openapi-v3 and graphql targets write the schema itself to stdout.
	return schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    rootDirs,
		SchemaFiles: opts.schemaFiles,
	})
}

// exportRootDirs returns the Go entity roots to scan, or none when the desired
// schema comes from --schema-file alone. The "." default applies only when no
// schema file was named, so --schema-file never silently merges whatever
// annotated Go files happen to sit in the working directory.
func exportRootDirs(opts exportOptions) ([]string, error) {
	if len(opts.schemaFiles) > 0 && !opts.rootDirExplicit {
		return nil, nil
	}
	rootDir, err := exportGoRootDir(opts)
	if err != nil {
		return nil, err
	}
	return []string{rootDir}, nil
}

// exportGoRootDir resolves and validates the single Go annotation root.
func exportGoRootDir(opts exportOptions) (string, error) {
	rootDir, err := pathguard.ResolveCLIPath(opts.rootDir)
	if err != nil {
		return "", fmt.Errorf("invalid root directory: %w", err)
	}
	if err := cmdutil.StatDir(rootDir); err != nil {
		return "", err
	}
	return rootDir, nil
}

// schemaFileFormat maps a schema file path to the --from value that names its
// format. The extension is the only declaration a schema file carries, and it
// is what schemaload dispatches on, so an explicit --from is checked against it
// rather than against the file's contents.
func schemaFileFormat(path string) (string, bool) {
	switch strings.ToLower(filepath.Ext(strings.TrimSpace(path))) {
	case ".yaml", ".yml":
		return exportFormatYAML, true
	case ".hcl":
		return exportFormatHCL, true
	case ".sql":
		return exportFormatSQL, true
	default:
		return "", false
	}
}

// validateExportSource checks the desired-schema source selection: whether the
// target can read a schema file at all, and whether an explicit --from agrees
// with the files named. With --from left at its default the extension decides,
// which is why the acceptance form of this command is --schema-file alone.
func validateExportSource(opts exportOptions) error {
	if opts.to == exportFormatHCL && len(opts.schemaFiles) > 0 {
		return fmt.Errorf(
			"--%s is not supported with --%s %s: that target rewrites the Go files it reads "+
				"(--%s removes their annotations), so its source is --%s; "+
				"use --%s openapi-v3, graphql, or protobuf to export a schema file",
			exportSchemaFileFlag, exportToFlag, exportFormatHCL,
			cleanupGoAnnotationsFlag, exportRootDirFlag, exportToFlag,
		)
	}
	if !opts.fromExplicit {
		return nil
	}
	switch opts.from {
	case exportFormatGo:
		if len(opts.schemaFiles) > 0 {
			return fmt.Errorf(
				"--%s %s reads Go annotations from --%s: drop --%s, or set it to the format of the --%s value",
				exportFromFlag, exportFormatGo, exportRootDirFlag, exportFromFlag, exportSchemaFileFlag,
			)
		}
		return nil
	case exportFormatYAML, exportFormatHCL, exportFormatSQL:
		return validateSchemaFileFormats(opts)
	case exportSourceDB:
		return fmt.Errorf(
			"--%s %s is not supported: an export reads a schema definition, not a live database; "+
				"run \"ptah introspect\" to generate annotated Go models from a database URL and export those",
			exportFromFlag, exportSourceDB,
		)
	default:
		return fmt.Errorf("unsupported --%s %q: expected %s, %s, %s, or %s",
			exportFromFlag, opts.from, exportFormatGo, exportFormatYAML, exportFormatHCL, exportFormatSQL)
	}
}

// validateSchemaFileFormats reports a --from that names a file format without a
// file to read, or that disagrees with the extension of one it was given.
func validateSchemaFileFormats(opts exportOptions) error {
	if len(opts.schemaFiles) == 0 {
		return fmt.Errorf("--%s %s requires --%s", exportFromFlag, opts.from, exportSchemaFileFlag)
	}
	for _, schemaFile := range opts.schemaFiles {
		// An OCI reference carries its format inside the artifact and has no
		// extension to check, so it is named here rather than falling into the
		// extension message below, which would read as "rename your registry
		// reference". The prefix test matches the one schemaload dispatches on.
		if strings.HasPrefix(schemaFile, ociartifact.Scheme) {
			return fmt.Errorf(
				"--%s cannot declare the format of the %s artifact %q, which records its own; omit --%s",
				exportFromFlag, ociartifact.Scheme, schemaFile, exportFromFlag,
			)
		}
		format, ok := schemaFileFormat(schemaFile)
		if !ok {
			return fmt.Errorf(
				"--%s %q has no recognized schema file extension: expected .yaml, .yml, .hcl, or .sql",
				exportSchemaFileFlag, schemaFile,
			)
		}
		if format != opts.from {
			return fmt.Errorf(
				"--%s %s does not match --%s %q, which is %s",
				exportFromFlag, opts.from, exportSchemaFileFlag, schemaFile, format,
			)
		}
	}
	return nil
}

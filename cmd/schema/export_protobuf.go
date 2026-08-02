package schema

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/fsdurable"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/protobufrender"
)

var (
	// protoPackagePattern is what buf lint's PACKAGE_LOWER_SNAKE_CASE and
	// PACKAGE_DEFINED between them require of a package.
	protoPackagePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)
	// protoVersionSuffixPattern is buf lint's PACKAGE_VERSION_SUFFIX. Ptah does
	// not own the caller's lint policy, so a package without it only warns.
	protoVersionSuffixPattern = regexp.MustCompile(`^v\d+$`)
	// goPackagePattern is a Go import path with the optional ";alias" form.
	// go_package is the only free-form string that reaches the generated file
	// body, so it is validated before anything is read or written.
	goPackagePattern = regexp.MustCompile(`^[A-Za-z0-9_.~-]+(/[A-Za-z0-9_.~-]+)*(;[A-Za-z_][A-Za-z0-9_]*)?$`)
	// protoFileNamePattern is buf lint's FILE_LOWER_SNAKE_CASE.
	protoFileNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*\.proto$`)
)

// validateProtobufExportOptions checks every protobuf-only flag before any file
// is read or written, so an invalid configuration can never touch the
// compatibility baseline.
func validateProtobufExportOptions(opts exportOptions) error {
	if strings.TrimSpace(opts.outPath) == "" {
		return fmt.Errorf("--%s is required for --%s %s: the previously generated file is the compatibility state",
			exportOutFlag, exportToFlag, exportFormatProtobuf)
	}
	pkg := strings.TrimSpace(opts.protoPackage)
	if pkg == "" {
		return fmt.Errorf("--%s is required for --%s %s", protoPackageFlag, exportToFlag, exportFormatProtobuf)
	}
	if !protoPackagePattern.MatchString(pkg) {
		return fmt.Errorf("invalid --%s %q: expected lower_snake.case segments, such as acme.inventory.v1",
			protoPackageFlag, pkg)
	}
	if goPkg := strings.TrimSpace(opts.protoGoPackage); goPkg != "" && !goPackagePattern.MatchString(goPkg) {
		return fmt.Errorf("invalid --%s %q: expected a Go import path, optionally followed by ;alias",
			protoGoPackageFlag, goPkg)
	}
	if _, err := parseRemovalPolicy(opts.protoTypeRemoval); err != nil {
		return err
	}
	if _, err := parseChangePolicy(opts.protoOnIncompatibleChange); err != nil {
		return err
	}
	if _, err := parseNameReusePolicy(opts.protoOnNameReuse); err != nil {
		return err
	}
	return nil
}

// rejectProtobufOnlyFlags keeps the protobuf-only flags from being silently
// accepted on another target, where they would have no effect.
func rejectProtobufOnlyFlags(opts exportOptions) error {
	for flag, value := range map[string]string{
		protoPackageFlag:   opts.protoPackage,
		protoGoPackageFlag: opts.protoGoPackage,
	} {
		if strings.TrimSpace(value) != "" {
			return fmt.Errorf("--%s is only supported with --%s %s", flag, exportToFlag, exportFormatProtobuf)
		}
	}
	for flag, spec := range map[string][2]string{
		protoTypeRemovalFlag:          {opts.protoTypeRemoval, string(protobufrender.RemovalError)},
		protoOnIncompatibleChangeFlag: {opts.protoOnIncompatibleChange, string(protobufrender.ChangeError)},
		protoOnNameReuseFlag:          {opts.protoOnNameReuse, string(protobufrender.NameReuseError)},
	} {
		if value := strings.TrimSpace(spec[0]); value != "" && value != spec[1] {
			return fmt.Errorf("--%s is only supported with --%s %s", flag, exportToFlag, exportFormatProtobuf)
		}
	}
	return nil
}

func parseRemovalPolicy(value string) (protobufrender.RemovalPolicy, error) {
	switch policy := protobufrender.RemovalPolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.RemovalError:
		return protobufrender.RemovalError, nil
	case protobufrender.RemovalTombstone, protobufrender.RemovalDrop:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s, %s, or %s", protoTypeRemovalFlag, value,
			protobufrender.RemovalError, protobufrender.RemovalTombstone, protobufrender.RemovalDrop)
	}
}

func parseChangePolicy(value string) (protobufrender.ChangePolicy, error) {
	switch policy := protobufrender.ChangePolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.ChangeError:
		return protobufrender.ChangeError, nil
	case protobufrender.ChangeRenumber:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s or %s", protoOnIncompatibleChangeFlag, value,
			protobufrender.ChangeError, protobufrender.ChangeRenumber)
	}
}

func parseNameReusePolicy(value string) (protobufrender.NameReusePolicy, error) {
	switch policy := protobufrender.NameReusePolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.NameReuseError:
		return protobufrender.NameReuseError, nil
	case protobufrender.NameReuseRelease:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s or %s", protoOnNameReuseFlag, value,
			protobufrender.NameReuseError, protobufrender.NameReuseRelease)
	}
}

// runProtobufExport renders the schema and replaces the output atomically. A
// failed render must never destroy the compatibility baseline, so nothing is
// written until the rendered bytes have been re-parsed successfully.
func runProtobufExport(cmd *cobra.Command, opts exportOptions, db *goschema.Database) error {
	outPath, err := pathguard.ResolveCLIPath(strings.TrimSpace(opts.outPath))
	if err != nil {
		return fmt.Errorf("invalid output path: %w", err)
	}

	removal, err := parseRemovalPolicy(opts.protoTypeRemoval)
	if err != nil {
		return err
	}
	change, err := parseChangePolicy(opts.protoOnIncompatibleChange)
	if err != nil {
		return err
	}
	nameReuse, err := parseNameReusePolicy(opts.protoOnNameReuse)
	if err != nil {
		return err
	}

	pkg := strings.TrimSpace(opts.protoPackage)
	errOut := cmd.ErrOrStderr()
	for _, warning := range lintAdvisories(outPath, pkg) {
		fmt.Fprintf(errOut, "warning: %s\n", warning)
	}

	previous, hasPrevious, err := readPreviousExport(outPath)
	if err != nil {
		return err
	}

	rendered, err := protobufrender.Render(cmd.Context(), db, protobufrender.Options{
		IncludeTables:        opts.includeTables,
		ExcludeTables:        opts.excludeTables,
		Package:              pkg,
		GoPackage:            strings.TrimSpace(opts.protoGoPackage),
		OutPath:              outPath,
		Previous:             previous,
		HasPrevious:          hasPrevious,
		TypeRemoval:          removal,
		OnIncompatibleChange: change,
		OnNameReuse:          nameReuse,
	})
	if err != nil {
		return err
	}

	for _, diagnostic := range rendered.Diagnostics {
		fmt.Fprintf(errOut, "%s: %s: %s\n", diagnostic.Severity, diagnostic.Path, diagnostic.Message)
	}

	if err := writeProtobufAtomically(outPath, rendered.Data); err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Exported Protobuf schema to %s\n", outPath)
	fmt.Fprintf(out, "Exported %d message(s), %d field(s), %d enum(s)\n",
		rendered.Messages, rendered.Fields, rendered.Enums)
	if rendered.Bootstrapped {
		fmt.Fprintln(out, "bootstrapped new compatibility history")
	}
	if len(rendered.Diagnostics) > 0 {
		fmt.Fprintf(out, "%d export warning(s) reported\n", len(rendered.Diagnostics))
	}
	return nil
}

// lintAdvisories reports the buf lint rules the chosen paths will trip. Ptah
// cannot see the caller's buf module root, so these can only ever be warnings.
func lintAdvisories(outPath, pkg string) []string {
	var warnings []string

	segments := strings.Split(pkg, ".")
	if last := segments[len(segments)-1]; !protoVersionSuffixPattern.MatchString(last) {
		warnings = append(warnings, fmt.Sprintf(
			"protobuf package %q does not end in a version segment; buf lint STANDARD reports PACKAGE_VERSION_SUFFIX for it (expected something like %s.v1)",
			pkg, pkg))
	}

	if base := filepath.Base(outPath); !protoFileNamePattern.MatchString(base) {
		warnings = append(warnings, fmt.Sprintf(
			"output file %q is not lower_snake_case.proto; buf lint STANDARD reports FILE_LOWER_SNAKE_CASE for it", base))
	}

	// Compare with forward slashes: ResolveCLIPath returns OS-native
	// separators, so a literal "/" comparison would warn on every Windows run.
	dir := filepath.ToSlash(filepath.Dir(outPath))
	want := strings.ReplaceAll(pkg, ".", "/")
	if !hasPathSuffix(dir, want) {
		warnings = append(warnings, fmt.Sprintf(
			"output directory %q does not end in %q; buf lint STANDARD reports PACKAGE_DIRECTORY_MATCH unless the file sits at that path relative to the buf module root",
			dir, want))
	}
	return warnings
}

// hasPathSuffix reports whether dir ends with want on a path-component
// boundary. A plain strings.HasSuffix would accept ".../myacme/inventory/v1"
// for package acme.inventory.v1 and stay silent when buf would complain.
func hasPathSuffix(dir, want string) bool {
	if dir == want {
		return true
	}
	return strings.HasSuffix(dir, "/"+want)
}

// readPreviousExport loads the existing output. The second result reports
// whether a file is present at all, which is kept separate from its length: a
// zero-byte file must be refused by the validation gate, not mistaken for the
// bootstrap case and silently renumbered from 1. Anything that cannot be read
// is an error, because treating an unreadable baseline as absent would do the
// same damage.
func readPreviousExport(outPath string) (data []byte, exists bool, err error) {
	data, err = os.ReadFile(outPath)
	if err == nil {
		return data, true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("read previous export %s: %w", outPath, err)
}

// writeProtobufAtomically publishes data at outPath without ever leaving a
// partial file behind, mirroring internal/migratesum's durable write: a failed
// generation must not destroy the compatibility baseline.
func writeProtobufAtomically(outPath string, data []byte) error {
	dir := filepath.Dir(outPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	file, err := os.CreateTemp(dir, "."+filepath.Base(outPath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary export: %w", err)
	}
	tempPath := file.Name()

	// The generated file is committed alongside the schema, so it uses the same
	// 0644 permissions as other generated artifacts.
	if err := file.Chmod(0o644); err != nil {
		return errors.Join(fmt.Errorf("prepare temporary export: %w", err), file.Close(), os.Remove(tempPath))
	}
	if _, err := file.Write(data); err != nil {
		return errors.Join(fmt.Errorf("write temporary export: %w", err), file.Close(), os.Remove(tempPath))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync temporary export: %w", err), file.Close(), os.Remove(tempPath))
	}
	if err := file.Close(); err != nil {
		return errors.Join(fmt.Errorf("close temporary export: %w", err), os.Remove(tempPath))
	}
	if err := fsdurable.ReplaceFile(tempPath, outPath); err != nil {
		return errors.Join(fmt.Errorf("publish export: %w", err), os.Remove(tempPath))
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync output directory: %w", err)
	}
	return nil
}

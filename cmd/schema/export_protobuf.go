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
	if _, err := parseFieldRemovalPolicy(opts.protoOnFieldRemoval); err != nil {
		return err
	}
	if _, err := parseNameReusePolicy(opts.protoOnNameReuse); err != nil {
		return err
	}
	if _, err := parseSplitPolicy(opts.protoSplit); err != nil {
		return err
	}
	if _, err := parseMovePolicy(opts.protoOnTypeMove); err != nil {
		return err
	}
	if _, err := parseCommentPolicy(opts.protoComments); err != nil {
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
		protoOnFieldRemovalFlag:       {opts.protoOnFieldRemoval, string(protobufrender.FieldRemovalError)},
		protoSplitFlag:                {opts.protoSplit, string(protobufrender.SplitNone)},
		protoOnTypeMoveFlag:           {opts.protoOnTypeMove, string(protobufrender.MoveError)},
		protoCommentsFlag:             {opts.protoComments, string(protobufrender.CommentsNone)},
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

func parseFieldRemovalPolicy(value string) (protobufrender.FieldRemovalPolicy, error) {
	switch policy := protobufrender.FieldRemovalPolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.FieldRemovalError:
		return protobufrender.FieldRemovalError, nil
	case protobufrender.FieldRemovalReserve:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s or %s", protoOnFieldRemovalFlag, value,
			protobufrender.FieldRemovalError, protobufrender.FieldRemovalReserve)
	}
}

func parseSplitPolicy(value string) (protobufrender.SplitPolicy, error) {
	switch policy := protobufrender.SplitPolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.SplitNone:
		return protobufrender.SplitNone, nil
	case protobufrender.SplitTable:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s or %s", protoSplitFlag, value,
			protobufrender.SplitNone, protobufrender.SplitTable)
	}
}

func parseMovePolicy(value string) (protobufrender.MovePolicy, error) {
	switch policy := protobufrender.MovePolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.MoveError:
		return protobufrender.MoveError, nil
	case protobufrender.MoveRelocate:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s or %s", protoOnTypeMoveFlag, value,
			protobufrender.MoveError, protobufrender.MoveRelocate)
	}
}

// parseCommentPolicy resolves --proto-comments. Suppression is all-or-nothing:
// a table comment can carry exactly the internal detail a column comment can, so
// a per-object form would report a boundary the published contract does not
// have. The value is enumerated rather than a boolean so a future policy can be
// added without renaming the flag.
func parseCommentPolicy(value string) (protobufrender.CommentPolicy, error) {
	switch policy := protobufrender.CommentPolicy(strings.TrimSpace(value)); policy {
	case "", protobufrender.CommentsNone:
		return protobufrender.CommentsNone, nil
	case protobufrender.CommentsAll:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid --%s %q: expected %s or %s", protoCommentsFlag, value,
			protobufrender.CommentsAll, protobufrender.CommentsNone)
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
	fieldRemoval, err := parseFieldRemovalPolicy(opts.protoOnFieldRemoval)
	if err != nil {
		return err
	}
	nameReuse, err := parseNameReusePolicy(opts.protoOnNameReuse)
	if err != nil {
		return err
	}
	split, err := parseSplitPolicy(opts.protoSplit)
	if err != nil {
		return err
	}
	move, err := parseMovePolicy(opts.protoOnTypeMove)
	if err != nil {
		return err
	}
	comments, err := parseCommentPolicy(opts.protoComments)
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
	siblings, err := readPreviousSiblings(outPath, previous)
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
		PreviousSiblings:     siblings,
		Split:                split,
		TypeRemoval:          removal,
		OnIncompatibleChange: change,
		OnNameReuse:          nameReuse,
		OnFieldRemoval:       fieldRemoval,
		OnTypeMove:           move,
		Comments:             comments,
	})
	if err != nil {
		return err
	}

	for _, diagnostic := range rendered.Diagnostics {
		fmt.Fprintf(errOut, "%s: %s: %s\n", diagnostic.Severity, diagnostic.Path, diagnostic.Message)
	}

	written, err := publishProtobufSet(outPath, rendered.Files, rendered.Removed)
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	for _, path := range written {
		fmt.Fprintf(out, "Exported Protobuf schema to %s\n", path)
	}
	for _, name := range rendered.Removed {
		fmt.Fprintf(out, "Removed %s\n", filepath.Join(filepath.Dir(outPath), name))
	}
	// The file count is only reported for a set, so the default single-file
	// summary stays exactly what it has always been.
	suffix := ""
	if len(rendered.Files) > 1 {
		suffix = fmt.Sprintf(" across %d file(s)", len(rendered.Files))
	}
	fmt.Fprintf(out, "Exported %d message(s), %d field(s), %d enum(s)%s\n",
		rendered.Messages, rendered.Fields, rendered.Enums, suffix)
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

// readPreviousSiblings loads every other file the previous export recorded in
// the anchor's manifest. A named file that is missing is an error rather than a
// bootstrap: its messages would look deleted and their field numbers would
// restart at 1, which is the exact damage the manifest exists to prevent.
//
// It is only called when an anchor exists; nil anchor bytes record no siblings,
// so a bootstrap run does not need a special case here.
func readPreviousSiblings(outPath string, anchor []byte) (map[string][]byte, error) {
	names, err := protobufrender.ManifestNames(anchor)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}
	dir := filepath.Dir(outPath)
	siblings := make(map[string][]byte, len(names))
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf(
					"%s lists %s as part of its export set, but that file is missing; restore it from version control, or delete the whole set to start a new compatibility history",
					outPath, name)
			}
			return nil, fmt.Errorf("read previous export %s: %w", filepath.Join(dir, name), err)
		}
		siblings[name] = data
	}
	return siblings, nil
}

// publishProtobufSet writes every file of the export and then deletes the files
// a previous export wrote that this one no longer contains. Each file is staged
// and renamed on its own, so no half-written file is ever visible.
//
// The anchor is written last and the deletions run after it, because the anchor
// carries the inventory of the set. A failure part-way through therefore leaves
// the old inventory in place, and the old inventory never names a file that is
// not on disk; writing the anchor first would leave it pointing at a member that
// was never written, which the next run has to refuse.
//
// It returns the paths written, anchor first, which is the order they are
// reported in.
func publishProtobufSet(outPath string, files []protobufrender.OutputFile, removed []string) ([]string, error) {
	dir := filepath.Dir(outPath)
	anchorPath := outPath
	written := make([]string, 0, len(files))
	for _, file := range files {
		if file.Anchor {
			continue
		}
		path := filepath.Join(dir, file.Name)
		if err := writeProtobufAtomically(path, file.Data); err != nil {
			return nil, err
		}
		written = append(written, path)
	}
	for _, file := range files {
		if !file.Anchor {
			continue
		}
		if err := writeProtobufAtomically(anchorPath, file.Data); err != nil {
			return nil, err
		}
	}
	for _, name := range removed {
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("remove superseded export %s: %w", path, err)
		}
	}
	return append([]string{anchorPath}, written...), nil
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

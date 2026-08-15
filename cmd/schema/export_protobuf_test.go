package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/internal/protobufrender"
)

// protoTestPackage is the well-formed package used by the happy-path tests: it
// carries the version suffix buf lint requires, so those tests can assert that
// no advisory fires.
const protoTestPackage = "acme.inventory.v1"

// runSchemaExport executes `schema export ...` with the two streams kept apart,
// so a stdout assertion can never be satisfied by a stderr warning and vice
// versa. The protobuf target splits its output that way deliberately: warnings
// and diagnostics go to stderr, the summary to stdout.
func runSchemaExport(args ...string) (stdout, stderr string, err error) {
	cmd := schema.NewSchemaCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{"export"}, args...))
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// resolvedTempDir returns a temporary directory with its symlinks already
// resolved. pathguard.ResolveCLIPath resolves them before the command echoes
// the destination back, and on macOS t.TempDir() sits under a symlinked /var,
// so the raw path never appears in the command output.
func resolvedTempDir(c *qt.C) string {
	c.Helper()
	resolved, err := filepath.EvalSymlinks(c.TempDir())
	c.Assert(err, qt.IsNil)
	return resolved
}

// leftoverTempFiles lists the atomic-write scratch files still present in dir.
// writeProtobufAtomically stages into ".<base>.*.tmp" before renaming, so any
// survivor means a run left partial state behind.
func leftoverTempFiles(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	var leftovers []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			leftovers = append(leftovers, entry.Name())
		}
	}
	return leftovers
}

// exportProtobufFixture writes the shared Go model into a fresh directory and
// returns the directory plus a well-formed output path whose directory matches
// protoTestPackage.
func exportProtobufFixture(c *qt.C) (dir, outPath string) {
	c.Helper()
	dir = resolvedTempDir(c)
	writeModel(c, dir)
	return dir, filepath.Join(dir, "proto", "acme", "inventory", "v1", "schema.proto")
}

// lastLines returns the final n lines of text, ignoring its trailing newline.
func lastLines(text string, n int) []string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	return lines[max(len(lines)-n, 0):]
}

// digestValueOf returns the value recorded on the file's content-digest line.
func digestValueOf(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		value, found := strings.CutPrefix(line, "// ptah:content-sha256=")
		if found {
			return value
		}
	}
	return ""
}

// writeCommentedModel writes a model carrying an internal table comment and a
// sensitive column comment, the fixture the comment policy exists for.
func writeCommentedModel(c *qt.C, dir string) {
	c.Helper()
	content := `package models

//ptah:schema:table name="users" comment="Internal: sharded by tenant_id; see runbook RB-42"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:field name="password_hash" type="VARCHAR(255)" not_null="true" comment="bcrypt hash, never expose"
	PasswordHash string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(content), 0o600), qt.IsNil)
}

// protoCommentLines returns every comment line of a generated .proto, which is
// the entire prose surface a published contract exposes.
func protoCommentLines(text string) []string {
	var out []string
	for line := range strings.SplitSeq(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			out = append(out, trimmed)
		}
	}
	return out
}

func TestSchemaExportProtobufOmitsSourceCommentsUnlessAsked(t *testing.T) {
	c := qt.New(t)
	dir := resolvedTempDir(c)
	writeCommentedModel(c, dir)
	outPath := filepath.Join(dir, "proto", "acme", "inventory", "v1", "schema.proto")

	exportArgs := func(extra ...string) []string {
		return append([]string{
			"--to", "protobuf",
			"--root-dir", dir,
			"--out", outPath,
			"--proto-package", protoTestPackage,
		}, extra...)
	}

	// Asking for them puts both in the published contract, and from there into
	// whatever protoc-gen-go produces from it. This is the opt-in.
	_, stderr, err := runSchemaExport(exportArgs("--proto-comments", "all")...)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	withComments, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(withComments), qt.Contains,
		"// Internal: sharded by tenant_id; see runbook RB-42\nmessage User {")
	c.Assert(string(withComments), qt.Contains,
		"  // bcrypt hash, never expose\n  string password_hash = 3;")

	// Without the flag — the default — the only comments left are the three
	// generated header lines. The fixture is the reason the default points this
	// way: a sharding note and "bcrypt hash, never expose" are written for
	// whoever reads the database, not for whoever consumes the contract.
	_, stderr, err = runSchemaExport(exportArgs()...)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	stripped, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	lines := protoCommentLines(string(stripped))
	c.Assert(lines, qt.HasLen, 3)
	c.Assert(lines[0], qt.Equals, "// Code generated by ptah schema export --to protobuf; DO NOT EDIT.")
	c.Assert(lines[1], qt.Equals, "// ptah:protobuf-export-version=2")
	c.Assert(strings.HasPrefix(lines[2], "// ptah:content-sha256="), qt.IsTrue)
	c.Assert(string(stripped), qt.Not(qt.Contains), "runbook RB-42")
	c.Assert(string(stripped), qt.Not(qt.Contains), "bcrypt")
	// Nothing else moved: every field keeps the number it was published under.
	c.Assert(string(stripped), qt.Contains,
		"message User {\n  int32 id = 1;\n  string email = 2;\n  string password_hash = 3;\n}\n")

	// Running again under the same policy is a byte-identical no-op.
	_, stderr, err = runSchemaExport(exportArgs()...)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	repeated, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(repeated, qt.DeepEquals, stripped)

	// Asking for them again restores both and renumbers nothing, because
	// comments were never part of the compatibility state.
	_, stderr, err = runSchemaExport(exportArgs("--proto-comments", "all")...)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	restored, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.DeepEquals, withComments)
}

func TestSchemaExportProtobufWritesFileAndBootstrapsHistory(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	stdout, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+outPath+"\n")
	c.Assert(stdout, qt.Contains, "Exported 1 message(s)")
	c.Assert(stdout, qt.Contains, "bootstrapped new compatibility history")
	// A well-formed package, file name and directory trip none of the advisory
	// rules; only the bootstrap and type-mapping diagnostics are expected.
	c.Assert(stderr, qt.Not(qt.Contains), "buf lint STANDARD")
	c.Assert(stderr, qt.Contains, "no previous export found at "+outPath)

	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	rendered := string(content)
	c.Assert(rendered, qt.Contains, "// Code generated by ptah schema export --to protobuf; DO NOT EDIT.\n")
	c.Assert(rendered, qt.Contains, "// ptah:protobuf-export-version=2\n")
	c.Assert(rendered, qt.Contains, "// ptah:content-sha256=")
	// The header sits at the foot of the file: as its leading comment,
	// protoc-gen-go copies it into every .pb.go it generates.
	c.Assert(strings.HasPrefix(rendered, "edition = \"2023\";\n"), qt.IsTrue)
	c.Assert(lastLines(rendered, 3), qt.DeepEquals, []string{
		"// Code generated by ptah schema export --to protobuf; DO NOT EDIT.",
		"// ptah:protobuf-export-version=2",
		"// ptah:content-sha256=" + digestValueOf(rendered),
	})
	c.Assert(rendered, qt.Contains, "package "+protoTestPackage+";\n")
	c.Assert(rendered, qt.Contains, "message User {\n")
	c.Assert(rendered, qt.Contains, "  int32 id = 1;\n")
	c.Assert(rendered, qt.Contains, "  string email = 2;\n")

	// The second run reads the file it just wrote, so the history is no longer
	// being bootstrapped and the caller must not be told that it is.
	stdout, stderr, err = runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+outPath+"\n")
	c.Assert(stdout, qt.Not(qt.Contains), "bootstrapped new compatibility history")
	c.Assert(stderr, qt.Not(qt.Contains), "no previous export found")
}

func TestSchemaExportProtobufRegenerationIsByteIdentical(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	first, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)

	_, stderr, err = runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	second, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)

	// Byte identity is the contract that lets the generated file be committed
	// and checked in CI: an unchanged schema must not produce a diff.
	c.Assert(second, qt.DeepEquals, first)
}

func TestSchemaExportProtobufEmitsGoPackageOption(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
		"--go-package", "github.com/acme/inventory/gen/acme/inventory/v1;inventoryv1",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	content, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains,
		"option go_package = \"github.com/acme/inventory/gen/acme/inventory/v1;inventoryv1\";\n")
}

func TestSchemaExportProtobufRejectsInvalidConfiguration(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing out",
			args:    []string{"--proto-package", protoTestPackage},
			wantErr: "--out is required for --to protobuf: the previously generated file is the compatibility state",
		},
		{
			name:    "missing proto package",
			args:    []string{"--out", outPath},
			wantErr: "--proto-package is required for --to protobuf",
		},
		{
			name:    "invalid proto package",
			args:    []string{"--out", outPath, "--proto-package", "Acme.Inventory"},
			wantErr: `invalid --proto-package "Acme.Inventory": expected lower_snake.case segments, such as acme.inventory.v1`,
		},
		{
			name:    "invalid go package",
			args:    []string{"--out", outPath, "--proto-package", protoTestPackage, "--go-package", "not a path!"},
			wantErr: `invalid --go-package "not a path!": expected a Go import path, optionally followed by ;alias`,
		},
		{
			name:    "unknown type removal policy",
			args:    []string{"--out", outPath, "--proto-package", protoTestPackage, "--proto-type-removal", "nuke"},
			wantErr: `invalid --proto-type-removal "nuke": expected error, tombstone, or drop`,
		},
		{
			name:    "unknown incompatible change policy",
			args:    []string{"--out", outPath, "--proto-package", protoTestPackage, "--proto-on-incompatible-change", "shrug"},
			wantErr: `invalid --proto-on-incompatible-change "shrug": expected error or renumber`,
		},
		{
			name:    "unknown name reuse policy",
			args:    []string{"--out", outPath, "--proto-package", protoTestPackage, "--proto-on-name-reuse", "keep"},
			wantErr: `invalid --proto-on-name-reuse "keep": expected error or release`,
		},
		{
			name:    "unknown comment policy",
			args:    []string{"--out", outPath, "--proto-package", protoTestPackage, "--proto-comments", "some"},
			wantErr: `invalid --proto-comments "some": expected all or none`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			args := append([]string{"--to", "protobuf", "--root-dir", dir}, tt.args...)

			stdout, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.wantErr), qt.Commentf("stdout:\n%s", stdout))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: "+tt.wantErr+"\n")
			// Nothing may be written while the configuration is still unusable.
			_, statErr := os.Stat(outPath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func TestSchemaExportProtobufOnlyFlagsRejectedOnOtherTargets(t *testing.T) {
	c := qt.New(t)
	dir, _ := exportProtobufFixture(c)

	tests := []struct {
		name string
		args []string
		flag string
	}{
		{name: "proto package", args: []string{"--proto-package", protoTestPackage}, flag: "--proto-package"},
		{name: "go package", args: []string{"--go-package", "github.com/acme/inventory"}, flag: "--go-package"},
		{name: "type removal", args: []string{"--proto-type-removal", "tombstone"}, flag: "--proto-type-removal"},
		{
			name: "incompatible change",
			args: []string{"--proto-on-incompatible-change", "renumber"},
			flag: "--proto-on-incompatible-change",
		},
		{name: "name reuse", args: []string{"--proto-on-name-reuse", "release"}, flag: "--proto-on-name-reuse"},
		// "all" rather than "none": none is the default now, and a flag left at
		// its default is indistinguishable from one never passed.
		{name: "comments", args: []string{"--proto-comments", "all"}, flag: "--proto-comments"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			args := append([]string{"--to", "graphql", "--root-dir", dir}, tt.args...)
			wantErr := tt.flag + " is only supported with --to protobuf"

			stdout, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(wantErr), qt.Commentf("stdout:\n%s", stdout))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: "+wantErr+"\n")
			// The rejected run must not have emitted a GraphQL schema either.
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

func TestSchemaExportProtobufWarnsAboutIgnoredTitle(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	stdout, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
		"--title", "Inventory API",
	)

	// --title belongs to the OpenAPI target; it is advisory here, never fatal.
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stderr, qt.Contains, "warning: --title is ignored for --to protobuf\n")
	c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+outPath+"\n")
	c.Assert(stdout, qt.Not(qt.Contains), "Inventory API")
	_, statErr := os.Stat(outPath)
	c.Assert(statErr, qt.IsNil)
}

func TestSchemaExportProtobufReportsBufLintAdvisories(t *testing.T) {
	tests := []struct {
		name string
		// out is the output path relative to the fixture directory, kept as
		// segments so filepath.Join builds it with the platform separator.
		out         []string
		pkg         string
		wantWarning string
	}{
		{
			name: "package without version suffix",
			out:  []string{"proto", "acme", "inventory", "schema.proto"},
			pkg:  "acme.inventory",
			wantWarning: `warning: protobuf package "acme.inventory" does not end in a version segment; ` +
				`buf lint STANDARD reports PACKAGE_VERSION_SUFFIX for it (expected something like acme.inventory.v1)`,
		},
		{
			name: "output file is not lower snake case",
			out:  []string{"proto", "acme", "inventory", "v1", "Schema.Proto"},
			pkg:  protoTestPackage,
			wantWarning: `warning: output file "Schema.Proto" is not lower_snake_case.proto; ` +
				`buf lint STANDARD reports FILE_LOWER_SNAKE_CASE for it`,
		},
		{
			name: "output directory does not match package",
			out:  []string{"schema.proto"},
			pkg:  protoTestPackage,
			// The directory is absolute and machine-specific, so only the
			// stable tail of the message is asserted. It is compared with
			// forward slashes on every platform by design.
			wantWarning: `does not end in "acme/inventory/v1"; buf lint STANDARD reports PACKAGE_DIRECTORY_MATCH`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := resolvedTempDir(c)
			writeModel(c, dir)
			outPath := filepath.Join(append([]string{dir}, tt.out...)...)

			stdout, stderr, err := runSchemaExport(
				"--to", "protobuf",
				"--root-dir", dir,
				"--out", outPath,
				"--proto-package", tt.pkg,
			)

			// Ptah cannot see the caller's buf module root, so every one of
			// these is advisory: the export still succeeds.
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+outPath+"\n")
			c.Assert(stderr, qt.Contains, tt.wantWarning)
			// Exactly one advisory fires: each case trips one rule only.
			c.Assert(strings.Count(stderr, "buf lint STANDARD"), qt.Equals, 1)
		})
	}
}

func TestSchemaExportProtobufRefusalPreservesPreviousFile(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	before, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)

	// Re-exporting under a different package is refused: the existing file is
	// the compatibility baseline for acme.inventory.v1 and rewriting it would
	// silently restart the numbering history under a new package name.
	stdout, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", "acme.warehouse.v1",
	)

	c.Assert(err, qt.ErrorIs, protobufrender.ErrPackageMismatch, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(err.Error(), qt.Contains,
		`file declares "acme.inventory.v1", --proto-package is "acme.warehouse.v1"`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "error: output file declares a different protobuf package")
	c.Assert(stdout, qt.Not(qt.Contains), "Exported Protobuf schema to")

	after, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.DeepEquals, before)
	c.Assert(leftoverTempFiles(c, filepath.Dir(outPath)), qt.HasLen, 0)
}

func TestSchemaExportProtobufPublishesReadableFileAndCleansTempFiles(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportProtobufFixture(c)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	info, err := os.Stat(outPath)
	c.Assert(err, qt.IsNil)
	// The file is committed alongside the schema, so it carries the same 0644
	// permissions as other generated artifacts rather than os.CreateTemp's 0600.
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o644))

	outDir := filepath.Dir(outPath)
	c.Assert(leftoverTempFiles(c, outDir), qt.HasLen, 0)
	entries, err := os.ReadDir(outDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Name(), qt.Equals, "schema.proto")

	// A rerun replaces the file through the same staging path, so the directory
	// must still hold exactly the published file afterwards.
	_, stderr, err = runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(leftoverTempFiles(c, outDir), qt.HasLen, 0)
	info, err = os.Stat(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o644))
}

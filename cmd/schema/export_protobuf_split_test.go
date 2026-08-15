package schema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/protobufrender"
)

// splitModelWithNickname is the two-table fixture the split tests start from.
// Two tables are the minimum that makes --proto-split=table observable at all,
// and the "nickname" column exists to be removed later so that the pinned
// numbering stops matching what a fresh export would allocate.
const splitModelWithNickname = `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="nickname" type="VARCHAR(64)"
	Nickname string

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="total" type="INTEGER"
	Total int
}
`

// splitModelWithoutNickname is the same schema with the middle column dropped,
// which leaves number 2 reserved and pushes "email" to 3. A from-scratch export
// of this schema would number email 2, so any test that asserts 3 separates a
// carried-over history from a restarted one.
const splitModelWithoutNickname = `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="total" type="INTEGER"
	Total int
}
`

// splitModelOrdersOnly drops the products table entirely, so the file that held
// its message has nothing left to hold.
const splitModelOrdersOnly = `package models

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="total" type="INTEGER"
	Total int
}
`

// splitModelSharedEnum has one named enum referenced from both tables, which is
// the case that forces a cross-file reference: protobuf cannot declare the enum
// twice, so it lives in the anchor and the table files import it.
const splitModelSharedEnum = `package models

//ptah:schema:table name="products"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="visibility" type="enum_visibility" not_null="true" default="visible"
	Visibility string
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="visibility" type="enum_visibility" not_null="true" default="visible"
	Visibility string
}

//ptah:schema:enum name="enum_visibility" values="visible,hidden"
type EnumVisibility struct{}
`

// splitModelClashingWithAnchor has a table whose message name derives the same
// file name the --out file already uses.
const splitModelClashingWithAnchor = `package models

//ptah:schema:table name="schemas"
type Schema struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`

// splitFixture writes source into a fresh directory and returns that directory
// plus a --out path whose directory matches protoTestPackage, so no buf lint
// advisory fires and the assertions stay about the split itself.
func splitFixture(c *qt.C, source string) (dir, outPath string) {
	c.Helper()
	dir = resolvedTempDir(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0o600), qt.IsNil)
	return dir, filepath.Join(dir, "proto", "acme", "inventory", "v1", "schema.proto")
}

// rewriteSplitModel replaces the fixture's source with another revision.
func rewriteSplitModel(c *qt.C, dir, source string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0o600), qt.IsNil)
}

// exportSplit runs the protobuf export with --proto-split=table.
func exportSplit(dir, outPath string, extra ...string) (stdout, stderr string, err error) {
	args := append([]string{
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
		"--proto-split", "table",
	}, extra...)
	return runSchemaExport(args...)
}

// protoNames lists the .proto files in dir, sorted.
func protoNames(c *qt.C, dir string) []string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.proto"))
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, filepath.Base(match))
	}
	sort.Strings(names)
	return names
}

// readProtoSet reads every .proto in dir, keyed by base name.
func readProtoSet(c *qt.C, dir string) map[string]string {
	c.Helper()
	set := map[string]string{}
	for _, name := range protoNames(c, dir) {
		body, err := os.ReadFile(filepath.Join(dir, name))
		c.Assert(err, qt.IsNil)
		set[name] = string(body)
	}
	return set
}

func TestSchemaExportProtobufSplitWritesOneFilePerTable(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	stdout, stderr, err := exportSplit(dir, outPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(protoNames(c, outDir), qt.DeepEquals, []string{"order.proto", "product.proto", "schema.proto"})
	c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+outPath+"\n")
	c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+filepath.Join(outDir, "order.proto")+"\n")
	c.Assert(stdout, qt.Contains, "Exported Protobuf schema to "+filepath.Join(outDir, "product.proto")+"\n")
	c.Assert(stdout, qt.Contains, "Exported 2 message(s), 5 field(s), 0 enum(s) across 3 file(s)\n")

	first := readProtoSet(c, outDir)
	// Each file is a compilation unit of its own: its own generated marker, its
	// own format version and its own digest. Only the anchor records the set.
	for _, name := range []string{"schema.proto", "order.proto", "product.proto"} {
		c.Assert(first[name], qt.Contains, "// Code generated by ptah schema export --to protobuf; DO NOT EDIT.\n")
		c.Assert(first[name], qt.Contains, "// ptah:protobuf-export-version=2\n")
		c.Assert(first[name], qt.Contains, "// ptah:content-sha256=")
		c.Assert(first[name], qt.Contains, "package "+protoTestPackage+";\n")
	}
	c.Assert(first["schema.proto"], qt.Contains, "// ptah:protobuf-export-files=order.proto,product.proto\n")
	c.Assert(first["order.proto"], qt.Not(qt.Contains), "// ptah:protobuf-export-files=")
	c.Assert(first["product.proto"], qt.Not(qt.Contains), "// ptah:protobuf-export-files=")
	c.Assert(first["product.proto"], qt.Contains, "message Product {\n")
	c.Assert(first["order.proto"], qt.Contains, "message Order {\n")
	c.Assert(first["schema.proto"], qt.Not(qt.Contains), "message ")

	// The second run reads the whole set back through the anchor's manifest, so
	// every file must be reproduced byte for byte and nothing may be bootstrapped.
	stdout, stderr, err = exportSplit(dir, outPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Not(qt.Contains), "bootstrapped new compatibility history")
	c.Assert(readProtoSet(c, outDir), qt.DeepEquals, first)
	c.Assert(leftoverTempFiles(c, outDir), qt.HasLen, 0)
}

func TestSchemaExportProtobufSplitKeepsASharedEnumInTheAnchor(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelSharedEnum)
	outDir := filepath.Dir(outPath)

	_, stderr, err := exportSplit(dir, outPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	first := readProtoSet(c, outDir)
	c.Assert(first["schema.proto"], qt.Contains, "enum Visibility {\n")
	c.Assert(first["product.proto"], qt.Not(qt.Contains), "enum Visibility {")
	c.Assert(first["order.proto"], qt.Not(qt.Contains), "enum Visibility {")
	// A file that names the enum imports the anchor by the path the package
	// requires, which is the same path buf resolves from the module root. The
	// reference itself stays bare: both files share one package.
	c.Assert(first["product.proto"], qt.Contains, "import \"acme/inventory/v1/schema.proto\";\n")
	c.Assert(first["product.proto"], qt.Contains, "  Visibility visibility = 2;\n")
	c.Assert(first["order.proto"], qt.Contains, "import \"acme/inventory/v1/schema.proto\";\n")

	// Regeneration reads that bare reference back and must recognize it as the
	// same type. Spelling it fully qualified on the way in would look like a
	// wire-incompatible type change and refuse every later export.
	stdout, stderr, err := exportSplit(dir, outPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(readProtoSet(c, outDir), qt.DeepEquals, first)
}

func TestSchemaExportProtobufSplitDigestProtectsEveryFile(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	_, stderr, err := exportSplit(dir, outPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	// A hand edit to a file that is not the one --out names must still be
	// refused, and must name that file: the digest header is per file, so the
	// whole set is protected rather than only the anchor.
	siblingPath := filepath.Join(outDir, "product.proto")
	tampered, err := os.ReadFile(siblingPath)
	c.Assert(err, qt.IsNil)
	// #nosec G703 -- The path is this test's own t.TempDir(), not external input.
	c.Assert(os.WriteFile(siblingPath, append(tampered, []byte("\n// hand edited\n")...), 0o600), qt.IsNil)
	before := readProtoSet(c, outDir)

	stdout, stderr, err := exportSplit(dir, outPath)

	c.Assert(err, qt.ErrorIs, protobufrender.ErrModified, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(err.Error(), qt.Contains, "product.proto: output file was modified since it was generated")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "error: product.proto: output file was modified")
	c.Assert(readProtoSet(c, outDir), qt.DeepEquals, before)
}

// The manifest is what a later run reads the set back through, so an edit that
// drops a member from it would otherwise be indistinguishable from an export
// that never wrote that member -- and the dropped file's field numbers would
// restart at 1. It is inside the digest-covered header block for exactly that
// reason, which is what this measures: the edit is refused as a modified file,
// not read as a smaller set.
func TestSchemaExportProtobufSplitDigestCoversTheManifest(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	_, stderr, err := exportSplit(dir, outPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	anchor, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	shrunk := strings.Replace(string(anchor),
		"// ptah:protobuf-export-files=order.proto,product.proto\n",
		"// ptah:protobuf-export-files=order.proto\n", 1)
	c.Assert(shrunk, qt.Not(qt.Equals), string(anchor))
	// #nosec G703 -- The path is this test's own t.TempDir(), not external input.
	c.Assert(os.WriteFile(outPath, []byte(shrunk), 0o600), qt.IsNil)
	before := readProtoSet(c, outDir)

	stdout, refusal, err := exportSplit(dir, outPath)

	c.Assert(err, qt.ErrorIs, protobufrender.ErrModified, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(err.Error(), qt.Contains, "output file was modified since it was generated")
	// Unprefixed, unlike the sibling refusal in
	// TestSchemaExportProtobufSplitDigestProtectsEveryFile: the anchor is the
	// file --out already named, so there is nothing to disambiguate.
	c.Assert(refusal, qt.Contains, "error: output file was modified since it was generated")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(readProtoSet(c, outDir), qt.DeepEquals, before)
}

func TestSchemaExportProtobufSplitRefusesMovingATypeBetweenFiles(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	before := readProtoSet(c, outDir)

	// Switching the split moves every message out of the anchor. Left alone that
	// reads as "removed from schema.proto, added to product.proto", which
	// restarts Product's field numbers at 1.
	stdout, refusal, err := exportSplit(dir, outPath)

	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(refusal, qt.Contains, "error: types would move between files")
	c.Assert(err.Error(), qt.Contains, `message "Order" from "schema.proto" to "order.proto"`)
	c.Assert(err.Error(), qt.Contains, `message "Product" from "schema.proto" to "product.proto"`)
	c.Assert(err.Error(), qt.Contains, "--proto-on-type-move=relocate")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Not(qt.Contains), "Exported Protobuf schema to")

	// A refusal must leave the baseline exactly as it was, with no half-written
	// member of the new set next to it.
	c.Assert(readProtoSet(c, outDir), qt.DeepEquals, before)
	c.Assert(protoNames(c, outDir), qt.DeepEquals, []string{"schema.proto"})
	c.Assert(leftoverTempFiles(c, outDir), qt.HasLen, 0)
}

func TestSchemaExportProtobufSplitRelocateCarriesPinnedNumbering(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	// Retiring the middle column leaves 2 reserved and email pinned at 3, which
	// is what separates a carried-over history from a restarted one.
	rewriteSplitModel(c, dir, splitModelWithoutNickname)
	_, stderr, err = runSchemaExport(
		"--to", "protobuf",
		"--root-dir", dir,
		"--out", outPath,
		"--proto-package", protoTestPackage,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(readProtoSet(c, outDir)["schema.proto"], qt.Contains, "  string email = 3;\n")

	stdout, stderr, err := exportSplit(dir, outPath, "--proto-on-type-move", "relocate")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stderr, qt.Contains, `message "Product" moved from "schema.proto" to "product.proto"`)
	c.Assert(stdout, qt.Contains, "across 3 file(s)")

	moved := readProtoSet(c, outDir)["product.proto"]
	// The pinned numbering travelled with the message: a bootstrapped file would
	// have numbered email 2 and carried no reservations at all.
	c.Assert(moved, qt.Contains, "  int32 id = 1;\n")
	c.Assert(moved, qt.Contains, "  string email = 3;\n")
	c.Assert(moved, qt.Contains, "  reserved 2;\n")
	c.Assert(moved, qt.Contains, "  reserved nickname;\n")
}

func TestSchemaExportProtobufSplitRemovesASupersededFile(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	_, stderr, err := exportSplit(dir, outPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	rewriteSplitModel(c, dir, splitModelOrdersOnly)
	stdout, stderr, err := exportSplit(dir, outPath, "--proto-type-removal", "drop")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "Removed "+filepath.Join(outDir, "product.proto")+"\n")
	// The file is gone and the anchor no longer advertises it, so the next run
	// cannot fail looking for a member of the set that is not there.
	c.Assert(protoNames(c, outDir), qt.DeepEquals, []string{"order.proto", "schema.proto"})
	c.Assert(readProtoSet(c, outDir)["schema.proto"], qt.Contains, "// ptah:protobuf-export-files=order.proto\n")

	_, stderr, err = exportSplit(dir, outPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
}

func TestSchemaExportProtobufSplitRefusesAMissingMemberOfTheSet(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)
	outDir := filepath.Dir(outPath)

	_, stderr, err := exportSplit(dir, outPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	// Deleting one file of the set must not be mistaken for deleting its table:
	// that would drop Product's pinned numbers and restart them at 1.
	c.Assert(os.Remove(filepath.Join(outDir, "product.proto")), qt.IsNil)

	stdout, stderr, err := exportSplit(dir, outPath)

	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(err.Error(), qt.Contains, "lists product.proto as part of its export set, but that file is missing")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "error: ")
	c.Assert(protoNames(c, outDir), qt.DeepEquals, []string{"order.proto", "schema.proto"})
}

func TestSchemaExportProtobufSplitRefusesAFileNameThatIsAlreadyTheAnchor(t *testing.T) {
	c := qt.New(t)
	dir := resolvedTempDir(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(splitModelClashingWithAnchor), 0o600), qt.IsNil)

	tests := []struct {
		name string
		out  []string
	}{
		// Same spelling, and a spelling that differs only in case: on a
		// case-insensitive filesystem the second is the same file as the first,
		// and writing both would leave whichever landed last.
		{name: "same name", out: []string{"proto", "acme", "inventory", "v1", "schema.proto"}},
		{name: "same name other case", out: []string{"proto", "acme", "inventory", "v1", "Schema.Proto"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			outPath := filepath.Join(append([]string{dir}, tt.out...)...)

			stdout, stderr, err := exportSplit(dir, outPath)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
			c.Assert(err.Error(), qt.Contains,
				`message(s) "Schema" would be written to "schema.proto", which is the file --out already names`)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: message(s) \"Schema\" would be written to")
			_, statErr := os.Stat(outPath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func TestSchemaExportProtobufSplitRejectsInvalidPolicyValues(t *testing.T) {
	c := qt.New(t)
	dir, outPath := splitFixture(c, splitModelWithNickname)

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "unknown split",
			args:    []string{"--proto-split", "everything"},
			wantErr: `invalid --proto-split "everything": expected none or table`,
		},
		{
			name:    "unknown type move policy",
			args:    []string{"--proto-on-type-move", "shrug"},
			wantErr: `invalid --proto-on-type-move "shrug": expected error or relocate`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			args := append([]string{
				"--to", "protobuf",
				"--root-dir", dir,
				"--out", outPath,
				"--proto-package", protoTestPackage,
			}, tt.args...)

			stdout, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.wantErr), qt.Commentf("stdout:\n%s", stdout))
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: "+tt.wantErr+"\n")
			_, statErr := os.Stat(outPath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func TestSchemaExportProtobufSplitFlagsRejectedOnOtherTargets(t *testing.T) {
	c := qt.New(t)
	dir, _ := splitFixture(c, splitModelWithNickname)

	tests := []struct {
		name string
		args []string
		flag string
	}{
		{name: "split", args: []string{"--proto-split", "table"}, flag: "--proto-split"},
		{name: "type move", args: []string{"--proto-on-type-move", "relocate"}, flag: "--proto-on-type-move"},
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
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

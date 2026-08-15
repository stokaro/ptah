package atlasreport_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasreport"
)

// `schema inspect` has two HCL output modes and only one of them used to carry
// the document's coverage record. Split/write applied the SAME block
// suppression, wrote the same objects out of the document, and dropped the
// three `// ptah:not-described` lines that say so, because every member is
// rebuilt from a parsed block and a leading comment belongs to no block.
//
// Measured on PostgreSQL 17.10 against a database holding an unreferenced
// `pgcrypto`, a standalone `ticket_seq` and an RLS policy `ticket_read`, on the
// commit these tests were added to correct:
//
//	inspect --format '{{ hcl . | split "schema" | write "out" }}'   exit 0
//	grep -rn ptah:not-described out                                 no match
//	schema apply --url <same db> --to file://out/public.hcl --auto-approve
//	                                                                exit 0
//	pg_extension pgcrypto        1 -> 0
//	pg_class     ticket_seq      1 -> 0
//	pg_policy    ticket_read     1 -> 0
//
// The objects were destroyed by inspecting a database and applying its own
// output back to it -- the round trip stokaro/ptah#1276 exists to prevent, one
// `--format` away from the path the single-document tests cover.

// TestSplitCarriesTheCoverageRecordIntoEveryMember pins the fix in the
// direction that matters: EVERY member, not the first one. `write` drops the
// members on the filesystem and the next process is handed one of them by path,
// so a record carried by a sibling is a record that is not there.
func TestSplitCarriesTheCoverageRecordIntoEveryMember(t *testing.T) {

	tests := []struct {
		name      string
		format    string
		wantFiles []string
	}{
		{
			name:      "split by schema",
			format:    `{{ hcl . | split "schema" }}`,
			wantFiles: []string{"public.hcl"},
		},
		{
			name:      "split by object, the default",
			format:    `{{ hcl . | split }}`,
			wantFiles: []string{"schemas/public.hcl", "tables/public_ticket.hcl"},
		},
		{
			name:      "split by type",
			format:    `{{ hcl . | split "type" }}`,
			wantFiles: []string{"schemas.hcl", "tables.hcl"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			output, err := atlasreport.RenderSchemaInspect(test.format, coverageSplitReport())

			c.Assert(err, qt.IsNil)
			members := txtarMembers(output.Text)
			c.Assert(memberPaths(members), qt.DeepEquals, test.wantFiles)
			for path, data := range members {
				c.Assert(leadingCommentLines(data), qt.DeepEquals, []string{
					"// ptah:not-described extension",
					"// ptah:not-described policy",
					"// ptah:not-described sequence",
				}, qt.Commentf("member %s", path))
			}
		})
	}
}

// TestSplitMembersDecodeBackToTheRecordTheDocumentDeclared closes the loop the
// directives exist for. A member is read by a different process through
// [coverage.DecodeHeader], so carrying the right TEXT is only half of it: the
// text has to decode to the same set the single-document render declares, and
// it has to sit in the leading comment header, which is the only place that
// decoder looks.
func TestSplitMembersDecodeBackToTheRecordTheDocumentDeclared(t *testing.T) {
	c := qt.New(t)

	whole, err := atlasreport.RenderSchemaInspect(`{{ hcl . }}`, coverageSplitReport())
	c.Assert(err, qt.IsNil)
	wantSet, err := coverage.DecodeHeader(whole.Text)
	c.Assert(err, qt.IsNil)
	c.Assert(wantSet, qt.DeepEquals, coverage.Set{}.WithKind(
		coverage.Extension, coverage.Policy, coverage.Sequence,
	))

	split, err := atlasreport.RenderSchemaInspect(`{{ hcl . | split }}`, coverageSplitReport())
	c.Assert(err, qt.IsNil)

	for path, data := range txtarMembers(split.Text) {
		t.Run(path, func(t *testing.T) {
			c := qt.New(t)
			got, err := coverage.DecodeHeader(data)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, wantSet)
		})
	}
}

// TestSplitWritePlansTheRecordIntoEveryExportedFile measures the deliverable
// rather than the rendered archive text: `write` is the step that makes each
// member an independently consumable desired state, and its planned file
// contents are exactly the bytes `schema apply --to file://out/public.hcl`
// later reads.
func TestSplitWritePlansTheRecordIntoEveryExportedFile(t *testing.T) {
	c := qt.New(t)

	output, err := atlasreport.RenderSchemaInspect(
		`{{ hcl . | split | write "out" }}`, coverageSplitReport(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(len(output.Files) >= 2, qt.IsTrue)
	for _, file := range output.Files {
		t.Run(file.Path, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(file.Dir, qt.Equals, "out")
			got, err := coverage.DecodeHeader(file.Data)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, coverage.Set{}.WithKind(
				coverage.Extension, coverage.Policy, coverage.Sequence,
			))
		})
	}
}

// TestSplitOfADocumentThatDescribesEverythingIsUnchanged is the other half of
// the pair. A record that claims nothing must add nothing: this is what
// PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 and every dialect with no refused block types
// produce, and a header prepended there would be a claim the document has no
// basis for.
func TestSplitOfADocumentThatDescribesEverythingIsUnchanged(t *testing.T) {
	c := qt.New(t)

	output, err := atlasreport.RenderSchemaInspect(`{{ hcl . | split "type" }}`, sampleSchemaInspectReport())

	c.Assert(err, qt.IsNil)
	for path, data := range txtarMembers(output.Text) {
		c.Assert(leadingCommentLines(data), qt.HasLen, 0, qt.Commentf("member %s", path))
	}
}

// TestSplitCarriesTheRecordWithSQLCommentSyntax pins the comment spelling per
// format. The directive grammar is shared across HCL and SQL on purpose, and a
// member written with the wrong marker is a member whose record no reader sees
// -- the same silent loss, one character wide.
func TestSplitCarriesTheRecordWithSQLCommentSyntax(t *testing.T) {
	c := qt.New(t)
	sqlText := "-- ptah:not-described sequence\n\nCREATE TABLE users (id int);\nCREATE TABLE posts (id int);\n"
	format := fmt.Sprintf(`{{ %q | split "type" }}`, sqlText)

	output, err := atlasreport.RenderSchemaInspect(format, sampleSchemaInspectReport())

	c.Assert(err, qt.IsNil)
	members := txtarMembers(output.Text)
	c.Assert(memberPaths(members), qt.DeepEquals, []string{"tables.sql"})
	for path, data := range members {
		got, err := coverage.DecodeHeader(data)
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Sequence), qt.Commentf("member %s", path))
	}
}

// TestSplitRefusesAMalformedRecordRatherThanDroppingIt keeps the closed-list
// promise across the split boundary. A directive this build cannot parse must
// not be silently discarded on the way into a member, because a record nothing
// understands reads as no record at all and the absence it protects becomes a
// removal.
func TestSplitRefusesAMalformedRecordRatherThanDroppingIt(t *testing.T) {
	c := qt.New(t)
	format := fmt.Sprintf(`{{ %q | split }}`, "// ptah:not-described wibble\n\nschema \"public\" {}\n")

	output, err := atlasreport.RenderSchemaInspect(format, sampleSchemaInspectReport())

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unknown coverage kind "wibble".*`)
	c.Assert(output.Text, qt.Equals, "")
}

// coverageSplitReport is an inspected PostgreSQL database carrying one of every
// block type the compatibility surface omits, so the render is in omit mode and
// declares the record the split has to carry.
func coverageSplitReport() *atlasreport.SchemaInspectReport {
	return atlasreport.NewSchemaInspectReport(
		&goschema.Database{
			Schemas: []goschema.Schema{{Name: "public"}},
			Tables:  []goschema.Table{{StructName: "Ticket", Name: "ticket", Schema: "public"}},
			Fields: []goschema.Field{
				{StructName: "Ticket", Name: "id", Type: "INTEGER", Primary: true},
			},
			Extensions: []goschema.Extension{{Name: "pgcrypto", IfNotExists: true}},
			Sequences:  []goschema.Sequence{{Name: "ticket_seq", Schema: "public"}},
			RLSPolicies: []goschema.RLSPolicy{
				{Name: "ticket_read", Table: "ticket", PolicyFor: "SELECT", UsingExpression: "true"},
			},
		},
		&types.DBSchema{},
		types.DBInfo{Dialect: platform.Postgres, Schema: "public"},
		nil,
		// The run did not choose its own scope, so the SQL format would leave the
		// schema row out. These cases render HCL and split it, which carries the
		// schema block either way; the value is the connected-schema one the
		// fixture represents.
		atlasreport.SchemaInspectReportOptions{OmitAtlasRefusedBlocks: true},
	)
}

// txtarMembers reads a rendered split archive back into path -> content.
func txtarMembers(archive string) map[string]string {
	members := map[string]string{}
	var path string
	var body strings.Builder
	flush := func() {
		if path != "" {
			members[path] = body.String()
		}
		body.Reset()
	}
	for line := range strings.SplitSeq(archive, "\n") {
		name, isHeader := txtarMemberName(line)
		if isHeader {
			flush()
			path = name
			continue
		}
		body.WriteString(line + "\n")
	}
	flush()
	return members
}

func txtarMemberName(line string) (string, bool) {
	rest, ok := strings.CutPrefix(line, "-- ")
	if !ok {
		return "", false
	}
	name, ok := strings.CutSuffix(rest, " --")
	return name, ok
}

func memberPaths(members map[string]string) []string {
	paths := make([]string, 0, len(members))
	for path := range members {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	return paths
}

// leadingCommentLines returns EVERY comment line of a document's leading
// header, verbatim, not only the ones that parse as directives.
//
// Every, because the empty-record row asserts the split adds nothing at all: a
// helper that skipped over comments it did not recognize would pass a member
// carrying an unrelated line the split had invented.
func leadingCommentLines(document string) []string {
	var lines []string
	for line := range strings.SplitSeq(document, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "//") && !strings.HasPrefix(trimmed, "#") && !strings.HasPrefix(trimmed, "--") {
			break
		}
		lines = append(lines, trimmed)
	}
	return lines
}

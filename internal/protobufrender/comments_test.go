package protobufrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/protobufrender"
)

// commentedSchema carries one internal table comment and one sensitive column
// comment. Both are written for whoever maintains the database, and neither is
// something a consumer of the published contract has any business reading.
func commentedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			Comment:    "Internal: sharded by tenant_id; see runbook RB-42",
		}},
		Fields: columns("User",
			goschema.Field{Name: "id", Type: "SERIAL", Primary: true},
			goschema.Field{Name: "email", Type: "VARCHAR(255)"},
			goschema.Field{Name: "password_hash", Type: "VARCHAR(255)", Comment: "bcrypt hash, never expose"},
		),
	}
}

// retiredSchema replaces the users table entirely, so the previous export's
// User message can only survive as a tombstone.
func retiredSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "Audit",
			Name:       "audits",
			Comment:    "Internal: retention is 30 days, enforced by cron",
		}},
		Fields: columns("Audit", goschema.Field{Name: "id", Type: "BIGINT"}),
	}
}

// commentOptions selects a comment policy on top of an existing option set.
func commentOptions(opts protobufrender.Options, policy protobufrender.CommentPolicy) protobufrender.Options {
	opts.Comments = policy
	return opts
}

// commentLines returns every comment line of a generated file. That is the
// whole surface this policy governs: a reader of the published contract sees
// these and nothing else of what the source schema had to say.
func commentLines(text string) []string {
	var out []string
	for line := range strings.SplitSeq(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			out = append(out, trimmed)
		}
	}
	return out
}

// assertGeneratedHeader pins the three lines every generated file carries, so a
// count assertion cannot be satisfied by the wrong three lines.
// assertGeneratedHeader checks the three generated lines at the END of the
// comment list. They live at the foot of the file since #1148: as the file's
// leading comment they were protoc-gen-go's leading comment too, so every
// consumer's .pb.go carried Ptah's content digest.
func assertGeneratedHeader(c *qt.C, lines []string) {
	c.Helper()
	c.Assert(len(lines) >= 3, qt.IsTrue, qt.Commentf("comment lines: %q", lines))
	header := lines[len(lines)-3:]
	c.Assert(header[0], qt.Equals, headerMarker)
	c.Assert(header[1], qt.Equals, versionLine)
	c.Assert(strings.HasPrefix(header[2], digestPrefix), qt.IsTrue)
}

func TestCommentsAllIsTheDefaultAndCopiesSourceProse(t *testing.T) {
	c := qt.New(t)

	byDefault := mustRenderText(c, commentedSchema(), baseOptions())
	explicit := mustRenderText(c, commentedSchema(), commentOptions(baseOptions(), protobufrender.CommentsAll))

	// The zero value must mean "all": a baseline generated before the policy
	// existed has to regenerate byte for byte.
	c.Assert(explicit, qt.Equals, byDefault)
	c.Assert(byDefault, qt.Contains,
		"// Internal: sharded by tenant_id; see runbook RB-42\nmessage User {")
	c.Assert(byDefault, qt.Contains,
		"  // bcrypt hash, never expose\n  string password_hash = 3;")
}

func TestCommentsNoneOmitsEverySourceComment(t *testing.T) {
	c := qt.New(t)

	text := mustRenderText(c, commentedSchema(), commentOptions(baseOptions(), protobufrender.CommentsNone))

	// Only the three generated header lines remain.
	lines := commentLines(text)
	c.Assert(lines, qt.HasLen, 3)
	assertGeneratedHeader(c, lines)

	// Suppression is all-or-nothing on purpose: the table comment and the column
	// comment carry the same kind of internal detail, so both go.
	c.Assert(text, qt.Not(qt.Contains), "runbook RB-42")
	c.Assert(text, qt.Not(qt.Contains), "bcrypt")
	c.Assert(section(text, "message User {"), qt.Equals,
		"message User {\n  int32 id = 1;\n  string email = 2;\n  string password_hash = 3;\n}")
}

func TestCommentsNoneKeepsPtahsOwnAccountOfTheContract(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, commentedSchema(), baseOptions())

	opts := commentOptions(withPrevious(baseline.Data), protobufrender.CommentsNone)
	opts.TypeRemoval = protobufrender.RemovalTombstone
	text := mustRenderText(c, retiredSchema(), opts)

	// The tombstone rationale is not source prose. It describes the generated
	// file itself, and without it a reader meets a bare reserved block with no
	// explanation of why the numbers are gone, so it survives the policy.
	c.Assert(text, qt.Contains,
		"// Removed from the source schema; retained for wire compatibility.\nmessage User {")
	lines := commentLines(text)
	c.Assert(lines, qt.HasLen, 4)
	assertGeneratedHeader(c, lines)
	// The tombstone rationale precedes the generated header, because the header
	// is the last thing in the file.
	c.Assert(lines[0], qt.Equals, "// Removed from the source schema; retained for wire compatibility.")

	// Both source comments are still gone, including the one on the table that
	// replaced the retired one.
	c.Assert(text, qt.Not(qt.Contains), "runbook RB-42")
	c.Assert(text, qt.Not(qt.Contains), "retention is 30 days")
}

func TestCommentPolicyIsNotPartOfTheCompatibilityState(t *testing.T) {
	c := qt.New(t)

	withComments := mustRender(c, commentedSchema(), baseOptions())

	stripped := mustRender(c, commentedSchema(),
		commentOptions(withPrevious(withComments.Data), protobufrender.CommentsNone))

	// A second run under the same policy is a no-op, which is what lets the
	// generated file be committed and checked in CI.
	again := mustRender(c, commentedSchema(),
		commentOptions(withPrevious(stripped.Data), protobufrender.CommentsNone))
	c.Assert(string(again.Data), qt.Equals, string(stripped.Data))

	// Dropping the policy restores the prose and moves no number: comments are
	// not compatibility state, so toggling this can never retire a field.
	restored := mustRender(c, commentedSchema(), withPrevious(stripped.Data))
	c.Assert(string(restored.Data), qt.Equals, string(withComments.Data))
	c.Assert(section(string(restored.Data), "message User {"), qt.Contains, "  string password_hash = 3;")

	// Nothing was reserved anywhere along the way. A reservation here would mean
	// the round trip had retired a live field.
	c.Assert(string(stripped.Data), qt.Not(qt.Contains), "reserved")
	c.Assert(string(restored.Data), qt.Not(qt.Contains), "reserved")
}

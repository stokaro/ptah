package atlasschema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/migration/safety"
)

// oraclePlanPath is the real `.plan.hcl` produced by Atlas for the
// schema-plan-file measurement
// scenario; it is the format oracle for the reader.
const oraclePlanPath = "testdata/atlas.plan.hcl"

// writtenGoldenPlanPath is the byte-exact document MarshalPlanFileHCL must
// produce for the measured scenario's native plan contents. It mirrors the
// mechanical Ptah-to-Atlas conversion validated against the official
// binary's parser during the measurement campaign.
const writtenGoldenPlanPath = "testdata/ptah-written-golden.plan.hcl"

// goldenPlan is the native plan for the measured scenario (the contents of
// the campaign's p.plan.json), used to exercise the HCL writer.
func goldenPlan() atlasschema.PlanFile {
	return atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "plan_6d78a9ec5390",
		Dialect:         "sqlite",
		FromFingerprint: "sha256:2ef81def17f625ec4fc7927e136e516022e244ab587bb702b5b71d38b05cbe27",
		ToFingerprint:   "sha256:c4fc6302f3cc08997acbb8b8d6ae52eabcbd9c6604a9835305035b1522e03b23",
		Statements: []atlasschema.PlanStatement{
			{
				SQL: "CREATE TABLE \"posts\" (\n  \"id\" integer PRIMARY KEY AUTOINCREMENT,\n  \"user_id\" integer NOT NULL,\n" +
					"  \"title\" TEXT NOT NULL,\n  CONSTRAINT \"fk_posts_user\" FOREIGN KEY (\"user_id\") REFERENCES \"users\" (\"id\")\n)",
				Severity: safety.Safe,
				Reason:   "does not remove data or tighten constraints",
			},
			{
				SQL:      `ALTER TABLE "users" ADD COLUMN "email" TEXT`,
				Severity: safety.Safe,
				Reason:   "does not remove data or tighten constraints",
			},
			{
				SQL:      `CREATE INDEX IF NOT EXISTS "idx_posts_user_id" ON "posts" ("user_id")`,
				Severity: safety.Safe,
				Reason:   "does not remove data or tighten constraints",
			},
		},
	}
}

func TestReadPlanDocumentParsesOracleAtlasPlan(t *testing.T) {
	c := qt.New(t)

	plan, format, err := atlasschema.ReadPlanDocument(oraclePlanPath)

	// Every field of the real Atlas-authored plan file lands in the plan
	// structure: the timestamp name, both base64 hashes verbatim, and the
	// three migration statements in file order with their comment lines.
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(plan.Name, qt.Equals, "20260801102801")
	c.Assert(plan.FromFingerprint, qt.Equals, "2Avyplv6jw8kAsH/g2YFPkfnp+UNBpomMXPUl/4R4+Q=")
	c.Assert(plan.ToFingerprint, qt.Equals, "YEugbm2aJqmXFA8dDrzmqLPC4tiNUrXe6YCrvazKOiY=")
	// The Atlas format records neither a format version, a dialect, nor
	// exclude patterns; those stay zero-valued.
	c.Assert(plan.FormatVersion, qt.Equals, 0)
	c.Assert(plan.Dialect, qt.Equals, "")
	c.Assert(plan.Exclude, qt.IsNil)
	c.Assert(plan.Destructive, qt.IsFalse)
	c.Assert(plan.Statements, qt.HasLen, 3)
	c.Assert(plan.Statements[0].SQL, qt.Equals,
		"-- Add column \"email\" to table: \"users\"\nALTER TABLE `users` ADD COLUMN `email` text NULL")
	c.Assert(plan.Statements[1].SQL, qt.Equals,
		"-- Create \"posts\" table\nCREATE TABLE `posts` (\n  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n"+
			"  `user_id` integer NOT NULL,\n  `title` text NOT NULL,\n"+
			"  CONSTRAINT `fk_posts_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION\n)")
	c.Assert(plan.Statements[2].SQL, qt.Equals,
		"-- Create index \"idx_posts_user_id\" to table: \"posts\"\nCREATE INDEX `idx_posts_user_id` ON `posts` (`user_id`)")
	for i, statement := range plan.Statements {
		c.Assert(statement.Severity, qt.Equals, safety.Safe, qt.Commentf("statement %d", i+1))
		c.Assert(statement.Reason, qt.Not(qt.Equals), "", qt.Commentf("statement %d", i+1))
	}
	// The Atlas hashes are foreign: base64 without an algorithm prefix, so
	// apply-time verification cannot recompute them.
	c.Assert(atlasschema.IsNativeFingerprint(plan.FromFingerprint), qt.IsFalse)
	c.Assert(atlasschema.IsNativeFingerprint(plan.ToFingerprint), qt.IsFalse)
}

func TestReadPlanDocumentReadsNativeJSONPlan(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "native.plan.json")
	document, err := atlasschema.MarshalPlanFile(goldenPlan())
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)

	plan, format, err := atlasschema.ReadPlanDocument(path)

	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatJSON)
	c.Assert(plan, qt.DeepEquals, goldenPlan())
	c.Assert(atlasschema.IsNativeFingerprint(plan.FromFingerprint), qt.IsTrue)
}

func TestDetectPlanFormat(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		want     atlasschema.PlanFormat
	}{
		{name: "json_document", contents: `{"format_version":1}`, want: atlasschema.PlanFormatJSON},
		{name: "json_with_leading_whitespace", contents: "\n\t {\"format_version\":1}", want: atlasschema.PlanFormatJSON},
		{name: "hcl_plan_block", contents: "plan \"x\" {\n}\n", want: atlasschema.PlanFormatHCL},
		{name: "hcl_with_leading_whitespace", contents: "\n  plan \"x\" {}", want: atlasschema.PlanFormatHCL},
		{name: "empty_defaults_to_hcl", contents: "", want: atlasschema.PlanFormatHCL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasschema.DetectPlanFormat([]byte(tt.contents)), qt.Equals, tt.want)
		})
	}
}

func TestMarshalPlanFileHCLMatchesGolden(t *testing.T) {
	c := qt.New(t)

	document, err := atlasschema.MarshalPlanFileHCL(goldenPlan())

	// The written document is byte-identical to the golden fixture, whose
	// shape Atlas parsed during the measurement campaign
	// (it aborted only at hash verification, which has no local recipe).
	c.Assert(err, qt.IsNil)
	golden, readErr := os.ReadFile(writtenGoldenPlanPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(document), qt.Equals, string(golden))
}

func TestMarshalPlanFileHCLMatchesOracleShape(t *testing.T) {
	c := qt.New(t)

	document, err := atlasschema.MarshalPlanFileHCL(goldenPlan())
	c.Assert(err, qt.IsNil)

	// Line-shape comparison against the real Atlas-authored plan file: same
	// block header, same aligned attributes, same heredoc opening and
	// closing. Only the values and the SQL body differ.
	oracle, readErr := os.ReadFile(oraclePlanPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(planDocumentShape(c, string(document)), qt.DeepEquals, planDocumentShape(c, string(oracle)))
}

// planDocumentShape reduces a plan document to its structural skeleton:
// quoted values become "V" and heredoc body lines collapse away, leaving
// exactly the lines whose shape the Atlas format fixes.
func planDocumentShape(c *qt.C, document string) []string {
	c.Helper()
	value := regexp.MustCompile(`"[^"]*"`)
	var shape []string
	inHeredoc := false
	for line := range strings.Lines(document) {
		line = strings.TrimRight(line, "\n")
		switch {
		case strings.HasSuffix(line, "<<-SQL"):
			inHeredoc = true
			shape = append(shape, value.ReplaceAllString(line, `"V"`))
		case inHeredoc && line == "  SQL":
			inHeredoc = false
			shape = append(shape, line)
		case inHeredoc:
			// SQL content is scenario-specific, not format shape.
		default:
			shape = append(shape, value.ReplaceAllString(line, `"V"`))
		}
	}
	return shape
}

func TestMarshalPlanFileHCLRoundTrips(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip.plan.hcl")
	document, err := atlasschema.MarshalPlanFileHCL(goldenPlan())
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)

	plan, format, err := atlasschema.ReadPlanDocument(path)

	// Reading back what the writer produced preserves the name, both
	// fingerprints, and every statement with its re-derived safety
	// classification. The dialect and format version are not representable
	// in the Atlas shape, so they come back zero-valued.
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(plan.Name, qt.Equals, goldenPlan().Name)
	c.Assert(plan.FromFingerprint, qt.Equals, goldenPlan().FromFingerprint)
	c.Assert(plan.ToFingerprint, qt.Equals, goldenPlan().ToFingerprint)
	c.Assert(plan.Statements, qt.DeepEquals, goldenPlan().Statements)
	c.Assert(plan.Destructive, qt.IsFalse)
	c.Assert(atlasschema.IsNativeFingerprint(plan.FromFingerprint), qt.IsTrue)
	c.Assert(atlasschema.IsNativeFingerprint(plan.ToFingerprint), qt.IsTrue)
}

func TestMarshalPlanFileHCLRefusesUnrepresentablePlans(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*atlasschema.PlanFile)
		want   string
	}{
		{
			name:   "exclude_patterns",
			mutate: func(plan *atlasschema.PlanFile) { plan.Exclude = []string{"users"} },
			want:   `the Atlas \.plan\.hcl format has no field for exclude patterns.*write the native JSON plan format.*or drop --exclude`,
		},
		{
			name: "heredoc_delimiter_line",
			mutate: func(plan *atlasschema.PlanFile) {
				plan.Statements[0].SQL = "CREATE TABLE x (\nSQL\n)"
			},
			want: `plan migration contains a line consisting of the heredoc delimiter "SQL".*`,
		},
		{
			name: "template_interpolation",
			mutate: func(plan *atlasschema.PlanFile) {
				plan.Statements[0].SQL = `INSERT INTO x VALUES ('${oops}')`
			},
			want: `plan migration contains an HCL template interpolation sequence.*`,
		},
		{
			name:   "name_needs_escaping",
			mutate: func(plan *atlasschema.PlanFile) { plan.Name = `a"b` },
			want:   `plan name .* contains characters that cannot be written verbatim.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			plan := goldenPlan()
			tt.mutate(&plan)

			_, err := atlasschema.MarshalPlanFileHCL(plan)

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

// TestMarshalPlanFileHCLRoundTripPropertyOverAdversarialSQL asserts the
// writer's whole contract over content designed to break the heredoc: either
// the document round-trips byte-for-byte through the reader, or the write is
// refused with a named error. Silent rewriting is never acceptable, because
// the plan file is the reviewed artifact and a rewritten statement is no
// longer what the operator approved.
func TestMarshalPlanFileHCLRoundTripPropertyOverAdversarialSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "plain_ddl",
			sql:  `CREATE TABLE t (id integer)`,
		},
		{
			name: "multiline_ddl_with_blank_line",
			sql:  "CREATE TABLE t (\n  id integer,\n\n  name text\n)",
		},
		{
			name: "leading_and_trailing_spaces_inside_lines",
			sql:  "CREATE TABLE t (\n      id integer\n)",
		},
		{
			name: "backslashes",
			sql:  `INSERT INTO t VALUES ('C:\path\to\file')`,
		},
		{
			name: "double_quotes_and_backticks",
			sql:  "CREATE TABLE \"t\" (`id` integer)",
		},
		{
			name: "sql_word_inside_a_longer_line",
			sql:  "CREATE TABLE t (\n  SQL_MODE text\n)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			plan := goldenPlan()
			plan.Statements = []atlasschema.PlanStatement{{
				SQL:      tt.sql,
				Severity: safety.Safe,
				Reason:   "test fixture",
			}}

			document, err := atlasschema.MarshalPlanFileHCL(plan)
			c.Assert(err, qt.IsNil)
			path := filepath.Join(t.TempDir(), "property.plan.hcl")
			c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)

			readBack, format, readErr := atlasschema.ReadPlanDocument(path)

			// The statement survives the heredoc verbatim, modulo the
			// trailing semicolon the writer normalizes onto every statement.
			c.Assert(readErr, qt.IsNil)
			c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
			c.Assert(readBack.Statements, qt.HasLen, 1)
			c.Assert(readBack.Statements[0].SQL, qt.Equals, strings.TrimSuffix(tt.sql, ";"))
		})
	}
}

// TestMarshalPlanFileHCLRefusesUnrepresentableMigrationContent is the other
// half of the writer's contract: content the heredoc cannot carry back
// verbatim is refused by name rather than silently rewritten, because a
// rewritten statement is no longer the one the operator reviewed.
func TestMarshalPlanFileHCLRefusesUnrepresentableMigrationContent(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			// A line that trims to the delimiter would close the heredoc.
			name: "sql_token_with_surrounding_spaces_on_its_own_line",
			sql:  "CREATE TABLE t (\n  SQL  \n)",
			want: `plan migration contains a line consisting of the heredoc delimiter "SQL".*`,
		},
		{
			// Both the carriage return and the delimiter line are refusals;
			// the CR is reported first because it is checked first.
			name: "sql_token_line_with_crlf",
			sql:  "CREATE TABLE t (\r\n  SQL\r\n)",
			want: `plan migration contains a carriage return.*`,
		},
		{
			name: "crlf_line_endings",
			sql:  "CREATE TABLE t (\r\n  id integer\r\n)",
			want: `plan migration contains a carriage return.*normalize the statement to LF line endings.*`,
		},
		{
			name: "lone_carriage_return",
			sql:  "CREATE TABLE t (\r  id integer)",
			want: `plan migration contains a carriage return.*`,
		},
		{
			name: "template_interpolation",
			sql:  `INSERT INTO t VALUES ('${var.x}')`,
			want: `plan migration contains an HCL template interpolation sequence.*`,
		},
		{
			name: "template_directive",
			sql:  `INSERT INTO t VALUES ('%{ if true }')`,
			want: `plan migration contains an HCL template interpolation sequence.*`,
		},
		{
			name: "invalid_utf8",
			sql:  "CREATE TABLE t (name text DEFAULT '\xff\xfe')",
			want: `plan migration contains invalid UTF-8.*`,
		},
		{
			// HCL leaves whitespace-only lines out of `<<-` stripping, so the
			// writer's indentation would come back inside the statement.
			name: "whitespace_only_line",
			sql:  "CREATE TABLE t (\n  \n  id integer\n)",
			want: `plan migration contains a whitespace-only line.*leave the line empty or write the native JSON plan format instead`,
		},
		{
			// A NUL byte is valid UTF-8, but the HCL reader truncates the
			// heredoc line at it, so a write/read round trip would silently
			// yield different — still parseable — SQL.
			name: "nul_byte",
			sql:  "CREATE TABLE t (name text DEFAULT 'a\x00b')",
			want: `plan migration contains a NUL byte.*remove it or write the native JSON plan format instead`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			plan := goldenPlan()
			plan.Statements = []atlasschema.PlanStatement{{
				SQL:      tt.sql,
				Severity: safety.Safe,
				Reason:   "test fixture",
			}}

			_, err := atlasschema.MarshalPlanFileHCL(plan)

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestReadPlanDocumentRejectsMalformedHCL(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		want     string
	}{
		{
			name:     "not_hcl",
			contents: "this is not a plan",
			want:     `parse plan file .*`,
		},
		{
			name:     "no_plan_block",
			contents: "env \"x\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c\"\n}\n",
			want:     `invalid plan file .*: unexpected block "env"; an Atlas plan file contains exactly one plan block`,
		},
		{
			name:     "two_plan_blocks",
			contents: "plan \"a\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n}\nplan \"b\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n}\n",
			want:     `invalid plan file .*: expected exactly one plan block, found 2 blocks`,
		},
		{
			name:     "missing_label",
			contents: "plan {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n}\n",
			want:     `invalid plan file .*: plan block requires exactly one name label, found 0`,
		},
		{
			name:     "missing_migration",
			contents: "plan \"a\" {\n  from = \"a\"\n  to = \"b\"\n}\n",
			want:     `invalid plan file .*: plan attribute "migration" is required`,
		},
		{
			name:     "unknown_attribute",
			contents: "plan \"a\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n  surprise = true\n}\n",
			want:     `invalid plan file .*: unknown plan attribute "surprise"`,
		},
		{
			name:     "non_string_attribute",
			contents: "plan \"a\" {\n  from = 1\n  to = \"b\"\n  migration = \"c;\"\n}\n",
			want:     `invalid plan file .*: plan attribute "from" must be a string`,
		},
		{
			name:     "nested_block",
			contents: "plan \"a\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n  lint {\n  }\n}\n",
			want:     `invalid plan file .*: unexpected nested "lint" block inside the plan block`,
		},
		{
			name:     "top_level_attribute",
			contents: "stray = true\nplan \"a\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n}\n",
			want:     `invalid plan file .*: unexpected top-level attribute "stray"; an Atlas plan file contains exactly one plan block`,
		},
		{
			name:     "empty_migration",
			contents: "plan \"a\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"\"\n}\n",
			want:     `invalid plan file .*: plan migration contains no statements`,
		},
		{
			name:     "empty_from",
			contents: "plan \"a\" {\n  from = \"\"\n  to = \"b\"\n  migration = \"c;\"\n}\n",
			want:     `invalid plan file .*: plan from fingerprint is required`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), "bad.plan.hcl")
			c.Assert(os.WriteFile(path, []byte(tt.contents), 0o600), qt.IsNil)

			_, _, err := atlasschema.ReadPlanDocument(path)

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestTimestampPlanName(t *testing.T) {
	c := qt.New(t)

	// The name is the Atlas-style UTC timestamp regardless of the local zone
	// of the input.
	instant := time.Date(2026, 8, 1, 10, 28, 1, 0, time.FixedZone("CEST", 2*60*60))

	c.Assert(atlasschema.TimestampPlanName(instant), qt.Equals, "20260801082801")
}

func TestIsNativeFingerprint(t *testing.T) {
	tests := []struct {
		name        string
		fingerprint string
		want        bool
	}{
		{
			name:        "ptah_sha256",
			fingerprint: "sha256:2ef81def17f625ec4fc7927e136e516022e244ab587bb702b5b71d38b05cbe27",
			want:        true,
		},
		{
			name:        "atlas_base64",
			fingerprint: "2Avyplv6jw8kAsH/g2YFPkfnp+UNBpomMXPUl/4R4+Q=",
			want:        false,
		},
		{name: "empty", fingerprint: "", want: false},
		{name: "junk", fingerprint: "sha256:xyz", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasschema.IsNativeFingerprint(tt.fingerprint), qt.Equals, tt.want)
		})
	}
}

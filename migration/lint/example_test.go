package lint_test

import (
	"fmt"
	"slices"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// ExampleLintFS is the compact first-touch path: lint an in-memory migration
// directory and print each finding through [lint.Describe]. The up migration
// drops a column and builds an index without CONCURRENTLY; Options.Dialect
// gates the PostgreSQL-specific rule on. Findings come back sorted by file,
// line, and rule code.
func ExampleLintFS() {
	fsys := fstest.MapFS{
		"0000000001_drop_email.up.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users DROP COLUMN email;\n" +
				"CREATE INDEX idx_users_name ON users (name);\n")},
		"0000000001_drop_email.down.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users ADD COLUMN email VARCHAR(255);\n")},
	}

	findings, err := lint.LintFS(fsys, lint.Options{Dialect: "postgres"})
	if err != nil {
		fmt.Println("lint failed:", err)
		return
	}
	for _, finding := range findings {
		fmt.Println(lint.Describe(finding))
	}

	// Output:
	// 0000000001_drop_email.up.sql:1 [warning] BC104: dropping a column retires a name application versions already deployed against the old schema still select and insert, so each of them starts failing the moment this migration commits, whether or not the column held any rows; deploy readers that no longer use the column first, then drop it in a later release (dropped column breaks deployed code)
	// 0000000001_drop_email.up.sql:1 [error] DS102: DROP COLUMN permanently deletes the column's data; deploy readers that no longer use the column first, then drop it in a later release (column dropped)
	// 0000000001_drop_email.up.sql:2 [warning] PG101: CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build; on a populated table use CREATE INDEX CONCURRENTLY outside a transaction (index built with a table lock)
}

// ExampleAnalyzeFS reaches for the richer result: prepared files carrying the
// semantic schema changes each up migration expresses ([lint.File.Changes],
// typed [lint.SchemaChange]) alongside the findings. One multi-action ALTER
// TABLE statement yields two changes, so a change count is not a statement
// count. The Analysis is an immutable snapshot: what an accessor hands back
// cannot be used to reach into it.
func ExampleAnalyzeFS() {
	fsys := fstest.MapFS{
		"0000000001_reshape.up.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users DROP COLUMN legacy, ADD COLUMN preferences TEXT;\n")},
		"0000000001_reshape.down.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users ADD COLUMN legacy TEXT, DROP COLUMN preferences;\n")},
	}

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{Dialect: "postgres"})
	if err != nil {
		fmt.Println("analysis failed:", err)
		return
	}
	for _, file := range analysis.Files() {
		if !file.IsUp {
			continue
		}
		fmt.Printf("%s: %d statement(s), %d change(s)\n", file.Name, len(file.Statements), len(file.Changes))
		for _, change := range file.Changes {
			fmt.Printf("  %s %s (statement %d)\n", change.Kind, change.Object, change.StatementIndex)
		}
	}
	for _, finding := range analysis.Findings() {
		fmt.Printf("finding %s at line %d\n", finding.Rule, finding.Line)
	}

	// Output:
	// 0000000001_reshape.up.sql: 1 statement(s), 2 change(s)
	//   drop legacy (statement 0)
	//   add preferences (statement 0)
	// finding BC104 at line 1
	// finding DS102 at line 1
}

// ExampleLoadConfigFS loads .ptah-lint.yaml ([lint.ConfigFileName]) from the
// migrations filesystem and threads Config.Dialect, Config.DisabledRules, and
// Config.Rules into [lint.Options] — the wiring an embedder does itself,
// because capture does not apply the lint policy automatically. Loading also
// canonicalizes the dialect ("postgresql" becomes "postgres"), which
// [lint.Options.Dialect] requires. Here the configuration disables PG101 and
// downgrades DS102 to a warning.
func ExampleLoadConfigFS() {
	fsys := fstest.MapFS{
		".ptah-lint.yaml": &fstest.MapFile{Data: []byte(
			"dialect: postgresql\n" +
				"disabled-rules:\n" +
				"  - PG101\n" +
				"rules:\n" +
				"  DS102:\n" +
				"    severity: warning\n")},
		"0000000001_drop_email.up.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users DROP COLUMN email;\n" +
				"CREATE INDEX idx_users_name ON users (name);\n")},
		"0000000001_drop_email.down.sql": &fstest.MapFile{Data: []byte(
			"ALTER TABLE users ADD COLUMN email VARCHAR(255);\n")},
	}

	cfg := must.Must(lint.LoadConfigFS(fsys, lint.ConfigFileName))
	fmt.Println("dialect:", cfg.Dialect)

	findings := must.Must(lint.LintFS(fsys, lint.Options{
		Dialect:     cfg.Dialect,
		Disabled:    cfg.DisabledRules,
		RuleConfigs: cfg.Rules,
	}))
	for _, finding := range findings {
		fmt.Println(finding.Severity, finding.Rule)
	}

	// Output:
	// dialect: postgres
	// warning BC104
	// warning DS102
}

// ExampleLintFS_extraRules runs a caller-provided rule through
// Options.ExtraRules, the request-scoped alternative to [lint.Register]. The
// rule scans [lint.Statement.Words], where bare keywords and identifiers are
// uppercased and a string literal stays one opaque verbatim word — so the
// TIMESTAMP column fires the rule and the literal mentioning TIMESTAMP cannot.
func ExampleLintFS_extraRules() {
	plainTimestamp := lint.Rule{
		Code:     "ORG101",
		Title:    "plain TIMESTAMP column",
		Severity: lint.SeverityWarning,
		CheckStatement: func(stmt *lint.Statement) (bool, string) {
			if slices.Contains(stmt.Words, "TIMESTAMP") {
				return true, "use TIMESTAMPTZ so stored instants keep their time zone"
			}
			return false, ""
		},
	}

	fsys := fstest.MapFS{
		"0000000001_events.up.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE events (recorded_at TIMESTAMP, id INTEGER);\n" +
				"INSERT INTO notes (body) VALUES ('a TIMESTAMP inside a literal');\n")},
		"0000000001_events.down.sql": &fstest.MapFile{Data: []byte(
			"DROP TABLE events;\n")},
	}

	findings := must.Must(lint.LintFS(fsys, lint.Options{
		Dialect:    "postgres",
		ExtraRules: []lint.Rule{plainTimestamp},
	}))
	for _, finding := range findings {
		fmt.Println(lint.Describe(finding))
	}

	// Output:
	// 0000000001_events.up.sql:1 [warning] ORG101: use TIMESTAMPTZ so stored instants keep their time zone (plain TIMESTAMP column)
}

// ExampleValidateOptions checks a lint policy without reading any migration
// file. An apply gate calls it before a no-work return or an execution
// override that would skip [lint.LintFS] or [lint.AnalyzeFS], so a selector
// naming no registered rule fails the run instead of silently disabling
// nothing.
func ExampleValidateOptions() {
	err := lint.ValidateOptions(lint.Options{
		Dialect:  "postgres",
		Disabled: []string{"ZZ999"},
	})
	fmt.Println(err)

	// Output:
	// rule selector "ZZ999" does not match any registered rule
}

// ExampleAnalyzeFS_baseline supplies the schema state a version starts from,
// the way a caller that replays the directory on a dev database does through
// [lint.Options.Baseline] and [lint.Options.BaselineIndexes]. The second
// migration drops an index and builds a unique one under another name;
// nothing in its text says the two cover the same column, and the state
// does. Run twice, once without the state, the same directory reports the
// build as a plain unique index and names the refinement it went without
// through [lint.Analysis.UnmetInputs]; [lint.Analysis.BaselineVersions]
// says which versions are worth reading. The dialect is SQLite so that the
// PostgreSQL lock rules stay out of the output.
func ExampleAnalyzeFS_baseline() {
	fsys := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE orders (id integer, email text);\nCREATE INDEX orders_email_idx ON orders (email);\n")},
		"2_unique.sql": &fstest.MapFile{Data: []byte(
			"DROP INDEX orders_email_idx;\nCREATE UNIQUE INDEX orders_email_uq ON orders (email);\n")},
	}
	opts := lint.Options{
		Dialect:   "sqlite",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
	}

	textOnly := must.Must(lint.AnalyzeFS(fsys, opts))
	fmt.Println("versions worth reading:", textOnly.BaselineVersions())
	for _, finding := range textOnly.Findings() {
		fmt.Println("without the state:", finding.Rule)
	}
	for _, unmet := range textOnly.UnmetInputs() {
		fmt.Printf("unmet: %s needs the %s\n", unmet.Rule, unmet.Input)
	}

	opts.Baseline = []lint.BaselineColumn{
		{Version: 2, Table: "orders", Name: "id", ColumnType: "integer"},
		{Version: 2, Table: "orders", Name: "email", ColumnType: "text"},
	}
	opts.BaselineIndexes = []lint.BaselineIndex{
		{Version: 2, Table: "orders", Name: "orders_email_idx", Parts: []lint.BaselineIndexPart{{Column: "email"}}},
	}
	refined := must.Must(lint.AnalyzeFS(fsys, opts))
	for _, finding := range refined.Findings() {
		fmt.Println("with the state:", finding.Rule)
	}
	fmt.Println("unmet with the state:", len(refined.UnmetInputs()))

	// Output:
	// versions worth reading: [2]
	// without the state: MF101
	// unmet: MF101 needs the baseline schema that refines the statement text
	// unmet: MF102 needs the baseline schema that refines the statement text
	// with the state: MF102
	// unmet with the state: 0
}

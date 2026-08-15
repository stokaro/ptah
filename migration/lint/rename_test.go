package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// analyzeRename runs one fixture under both command surfaces so a test can
// assert on the pair. Every rename test needs both halves: a rename is one
// event described differently per surface, so an assertion on either surface
// alone cannot show that the other one is still intact.
func analyzeRename(c *qt.C, files map[string]string) (native, atlas []lint.Finding) {
	c.Helper()
	fsys := fixture(files)
	nativeAnalysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "postgres",
	})
	c.Assert(err, qt.IsNil)
	atlasAnalysis, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "postgres",
	})
	c.Assert(err, qt.IsNil)
	return nativeAnalysis.Findings(), atlasAnalysis.Findings()
}

// severitiesOf collects finding severities, keeping finding order.
func severitiesOf(findings []lint.Finding) []lint.Severity {
	severities := make([]lint.Severity, 0, len(findings))
	for _, finding := range findings {
		severities = append(severities, finding.Severity)
	}
	return severities
}

// TestAnalyzeFS_RenameClassificationIsScopedToTheSurface covers issue #1074
// part 1. A rename retires a logical name; the compatibility surface has to
// classify that as a destructive change to the retired name, because the
// analyzer it matches does. The native surface keeps BC101's prose.
//
// Every form the scanner recognizes is a row, because they reach different
// branches: the table forms differ in whether the new name follows TO, follows
// nothing (MySQL), or sits in a standalone statement, and the column forms
// differ in whether the COLUMN keyword is present. A single ALTER TABLE ...
// RENAME COLUMN row would leave all of those unmeasured.
//
// Reverting the change turns every atlas column below into []string{"BC101"}.
func TestAnalyzeFS_RenameClassificationIsScopedToTheSurface(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		native []string
		atlas  []string
	}{
		{
			name:   "column rename",
			sql:    "ALTER TABLE users RENAME COLUMN id TO oid;",
			native: []string{"BC101"},
			atlas:  []string{"DS102"},
		},
		{
			name:   "column rename without the COLUMN keyword",
			sql:    "ALTER TABLE users RENAME email TO email_address;",
			native: []string{"BC101"},
			atlas:  []string{"DS102"},
		},
		{
			name:   "column rename behind IF EXISTS ONLY",
			sql:    "ALTER TABLE IF EXISTS ONLY users RENAME COLUMN email TO email_old;",
			native: []string{"BC101"},
			atlas:  []string{"DS102"},
		},
		{
			name:   "table rename",
			sql:    "ALTER TABLE users RENAME TO accounts;",
			native: []string{"BC101"},
			atlas:  []string{"DS101"},
		},
		{
			name:   "table rename spelled AS",
			sql:    "ALTER TABLE users RENAME AS accounts;",
			native: []string{"BC101"},
			atlas:  []string{"DS101"},
		},
		{
			name:   "table rename without TO",
			sql:    "ALTER TABLE users RENAME users_archive;",
			native: []string{"BC101"},
			atlas:  []string{"DS101"},
		},
		{
			name:   "standalone table rename",
			sql:    "RENAME TABLE users TO users_archive;",
			native: []string{"BC101"},
			atlas:  []string{"DS101"},
		},
		{
			name:   "standalone table rename of several tables",
			sql:    "RENAME TABLE users TO users_archive, pets TO pets_archive;",
			native: []string{"BC101"},
			atlas:  []string{"DS101", "DS101"},
		},
		{
			// Renames of objects deployed code never names are silent on both
			// surfaces, and are silent on the analyzer this tool matches too.
			name:   "constraint rename",
			sql:    "ALTER TABLE users RENAME CONSTRAINT users_id_chk TO users_id_positive;",
			native: []string{},
			atlas:  []string{},
		},
		{
			name:   "index rename",
			sql:    "ALTER TABLE users RENAME INDEX idx_old TO idx_new;",
			native: []string{},
			atlas:  []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native, atlas := analyzeRename(c, map[string]string{"1_rename.sql": test.sql + "\n"})
			c.Assert(rulesOf(native), qt.DeepEquals, test.native)
			c.Assert(rulesOf(atlas), qt.DeepEquals, test.atlas)
		})
	}
}

// TestAnalyzeFS_RenameNamesTheRetiredNameNotTheNewOne pins subject identity:
// the object the diagnostic is about is the name the rename takes away, spelled
// as the source spells it and qualified as the source qualifies it.
//
// The "reuses a name dropped earlier" row is the one that separates retired-name
// from new-name: the statement retires `id` and introduces `oid`, and `oid` is
// also the name a previous statement dropped, so a subject built from the new
// name or from the previously dropped name would both look plausible.
//
// Reverting the change leaves BC101 findings, which carry no subjects at all,
// so every row fails on an empty subject list.
func TestAnalyzeFS_RenameNamesTheRetiredNameNotTheNewOne(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		rules []string
		want  [][]lint.Subject
	}{
		{
			name:  "column rename",
			sql:   "ALTER TABLE users RENAME COLUMN id TO oid;",
			rules: []string{"DS102"},
			want:  [][]lint.Subject{{{Kind: lint.SubjectColumn, Name: "id", Parent: "users"}}},
		},
		{
			name:  "column rename keeps identifier spelling and table qualification",
			sql:   `ALTER TABLE public."Users" RENAME COLUMN "Id" TO oid;`,
			rules: []string{"DS102"},
			want:  [][]lint.Subject{{{Kind: lint.SubjectColumn, Name: `"Id"`, Parent: `public."Users"`}}},
		},
		{
			name:  "table rename",
			sql:   "ALTER TABLE users RENAME TO accounts;",
			rules: []string{"DS101"},
			want:  [][]lint.Subject{{{Kind: lint.SubjectTable, Name: "users"}}},
		},
		{
			name:  "table rename keeps identifier spelling and qualification",
			sql:   `ALTER TABLE public."Users" RENAME TO "Accounts";`,
			rules: []string{"DS101"},
			want:  [][]lint.Subject{{{Kind: lint.SubjectTable, Name: `public."Users"`}}},
		},
		{
			name:  "column rename that reuses a name dropped earlier",
			sql:   "ALTER TABLE users DROP COLUMN oid;\nALTER TABLE users RENAME COLUMN id TO oid;",
			rules: []string{"DS102", "DS102"},
			want: [][]lint.Subject{
				{{Kind: lint.SubjectColumn, Name: "oid", Parent: "users"}},
				{{Kind: lint.SubjectColumn, Name: "id", Parent: "users"}},
			},
		},
		{
			// A rename followed by an add-back of the retired name in the same
			// file is still reported: the old name is gone at the moment the
			// rename runs, and the analyzer this tool matches reports it too.
			name:  "column rename followed by an add-back of the old name",
			sql:   "ALTER TABLE users RENAME COLUMN id TO oid;\nALTER TABLE users ADD COLUMN id int;",
			rules: []string{"DS102"},
			want:  [][]lint.Subject{{{Kind: lint.SubjectColumn, Name: "id", Parent: "users"}}},
		},
		{
			// Measured on MySQL, whose grammar accepts the multi-clause form:
			// one diagnostic naming every renamed column, in clause order, under
			// a single suggested fix -- the same shape a multi-clause DROP COLUMN
			// produces, and the opposite of the table form below.
			name:  "several column renames in one statement are one finding",
			sql:   "ALTER TABLE users RENAME COLUMN nick TO handle, RENAME COLUMN email TO mail;",
			rules: []string{"DS102"},
			want: [][]lint.Subject{{
				{Kind: lint.SubjectColumn, Name: "nick", Parent: "users"},
				{Kind: lint.SubjectColumn, Name: "email", Parent: "users"},
			}},
		},
		{
			// Also measured on MySQL: one diagnostic per renamed table, ordered
			// by logical name rather than by the order the pairs are written,
			// exactly as a multi-target DROP TABLE is ordered. Written users
			// first, reported pets first.
			name:  "several table renames in one statement are ordered by name",
			sql:   "RENAME TABLE users TO accounts, pets TO animals;",
			rules: []string{"DS101", "DS101"},
			want: [][]lint.Subject{
				{{Kind: lint.SubjectTable, Name: "pets"}},
				{{Kind: lint.SubjectTable, Name: "users"}},
			},
		},
		{
			// The per-statement scope of that sort: two table renames written as
			// two statements keep source order, so "users" leads even though
			// "pets" sorts first.
			name:  "table renames in separate statements keep source order",
			sql:   "ALTER TABLE users RENAME TO accounts;\nALTER TABLE pets RENAME TO animals;",
			rules: []string{"DS101", "DS101"},
			want: [][]lint.Subject{
				{{Kind: lint.SubjectTable, Name: "users"}},
				{{Kind: lint.SubjectTable, Name: "pets"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, atlas := analyzeRename(c, map[string]string{"1_rename.sql": test.sql + "\n"})
			c.Assert(rulesOf(atlas), qt.DeepEquals, test.rules)
			c.Assert(subjectsOf(atlas), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_RenameIsErrorGradeOnTheCompatibilitySurface pins the severity
// that decides the process exit code. Severity is asserted apart from the rule
// code on purpose: a finding can carry the right code and the wrong grade, and
// then the report says "destructive changes detected" while the command exits
// 0.
//
// Reverting the change yields "warning" on the compatibility row.
func TestAnalyzeFS_RenameIsErrorGradeOnTheCompatibilitySurface(t *testing.T) {
	c := qt.New(t)

	native, atlas := analyzeRename(c, map[string]string{
		"1_rename.sql": "ALTER TABLE users RENAME COLUMN id TO oid;\n",
	})

	c.Assert(severitiesOf(native), qt.DeepEquals, []lint.Severity{lint.SeverityWarning})
	c.Assert(severitiesOf(atlas), qt.DeepEquals, []lint.Severity{lint.SeverityError})
}

// TestAnalyzeFS_RenamingAnObjectThisFileCreatedIsExempt covers the exemption
// measured on both rename forms: no deployed application version ever saw a
// name this same migration introduced, so retiring it breaks nothing.
//
// Each exempt row is paired with a control that differs only in where the
// CREATE lives. Without the control an exemption bug that silenced every rename
// would look like a pass.
//
// Reverting the change makes every exempt row report BC101 on both surfaces.
func TestAnalyzeFS_RenamingAnObjectThisFileCreatedIsExempt(t *testing.T) {
	tests := []struct {
		name   string
		files  map[string]string
		native []string
		atlas  []string
	}{
		{
			name: "column of a table created in this file",
			files: map[string]string{
				"1_rename.sql": "CREATE TABLE users (id int);\nALTER TABLE users RENAME COLUMN id TO oid;\n",
			},
			native: []string{},
			atlas:  []string{},
		},
		{
			name: "control: column of a table created in an earlier file",
			files: map[string]string{
				"1_init.sql":   "CREATE TABLE users (id int);\n",
				"2_rename.sql": "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			},
			native: []string{"BC101"},
			atlas:  []string{"DS102"},
		},
		{
			name: "table created in this file",
			files: map[string]string{
				"1_rename.sql": "CREATE TABLE staging (id int);\nALTER TABLE staging RENAME TO users;\n",
			},
			native: []string{},
			atlas:  []string{},
		},
		{
			name: "control: table created in an earlier file",
			files: map[string]string{
				"1_init.sql":   "CREATE TABLE staging (id int);\n",
				"2_rename.sql": "ALTER TABLE staging RENAME TO users;\n",
			},
			native: []string{"BC101"},
			atlas:  []string{"DS101"},
		},
		{
			name: "control: created after the rename, so not yet created when it runs",
			files: map[string]string{
				"1_rename.sql": "ALTER TABLE users RENAME COLUMN id TO oid;\nCREATE TABLE users (id int);\n",
			},
			native: []string{"BC101"},
			atlas:  []string{"DS102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native, atlas := analyzeRename(c, test.files)
			c.Assert(rulesOf(native), qt.DeepEquals, test.native)
			c.Assert(rulesOf(atlas), qt.DeepEquals, test.atlas)
		})
	}
}

// TestAnalyzeFS_UnreadableRenameStillReports pins the fail-closed path. A
// statement that is recognizably a rename but whose retired name cannot be read
// still reports, with no subject rather than a guessed one: an unreadable name
// is not evidence that nothing is being renamed.
//
// Reverting the change reports these as BC101, which is warning-grade and exits
// 0.
func TestAnalyzeFS_UnreadableRenameStillReports(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		rules []string
	}{
		{
			name:  "column rename with no target",
			sql:   "ALTER TABLE users RENAME COLUMN;",
			rules: []string{"DS102"},
		},
		{
			name:  "standalone rename with an unreadable pair",
			sql:   "RENAME TABLE users TO users_archive, pets;",
			rules: []string{"DS101", "DS101"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, atlas := analyzeRename(c, map[string]string{"1_rename.sql": test.sql + "\n"})
			c.Assert(rulesOf(atlas), qt.DeepEquals, test.rules)
			c.Assert(severitiesOf(atlas), qt.DeepEquals, []lint.Severity{lint.SeverityError, lint.SeverityError}[:len(test.rules)])
		})
	}
}

// TestAnalyzeFS_UnreadableRenameCarriesNoSubject keeps the fail-closed finding
// from carrying an invented object. It is separate from the test above because
// the code and the subject list fail independently: a finding can be routed to
// the right rule and still name the wrong thing.
//
// The rule code is asserted here too. Without it the test passes on master --
// the BC101 finding it reported instead also carries no subjects -- so the
// empty subject list on its own proves nothing about which rule produced it.
// Reverting the change fails the first assertion with BC101.
func TestAnalyzeFS_UnreadableRenameCarriesNoSubject(t *testing.T) {
	c := qt.New(t)

	_, atlas := analyzeRename(c, map[string]string{
		"1_rename.sql": "ALTER TABLE users RENAME COLUMN;\n",
	})

	c.Assert(rulesOf(atlas), qt.DeepEquals, []string{"DS102"})
	c.Assert(subjectsOf(atlas), qt.DeepEquals, [][]lint.Subject{nil})
}

// TestAnalyzeFS_RenameIsSuppressedByTheDestructiveSelectorOnly pins which Atlas
// analyzer owns a rename, measured rather than inferred: with `-- atlas:nolint
// destructive` above a column rename the pinned community binary reports
// nothing and exits 0, and with `-- atlas:nolint incompatible` above the same
// rename it still reports DS103 and exits 1.
//
// That is what forces the destructive classification into the DS family. The
// selectors are keyed by rule family -- destructive covers DS and CD,
// incompatible covers BC -- so a rename carrying a BC code on this surface is
// silenced by `incompatible`, which widens what a suppression directive covers.
// Ptah did exactly that before this change.
//
// Reverting it swaps both rows: `incompatible` silences the rename and
// `destructive` does not.
func TestAnalyzeFS_RenameIsSuppressedByTheDestructiveSelectorOnly(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		want     []string
	}{
		{name: "destructive owns the rename", selector: "destructive", want: []string{}},
		{name: "incompatible does not", selector: "incompatible", want: []string{"DS102"}},
		{name: "concurrent_index does not", selector: "concurrent_index", want: []string{"DS102"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, atlas := analyzeRename(c, map[string]string{
				"1_init.sql": "CREATE TABLE users (id int);\n",
				"2_rename.sql": "-- atlas:nolint " + test.selector + "\n" +
					"ALTER TABLE users RENAME COLUMN id TO oid;\n",
			})
			c.Assert(rulesOf(atlas), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_DropColumnStillReportsOnASameFileCreatedTable is a
// non-interference control for moving DS102 from a statement rule to a file
// rule. The rename form needed file scope to see what this file created; a
// plain DROP COLUMN must not pick up that exemption as a side effect, because
// relaxing a destructive check is not something to do accidentally.
//
// This control cannot be validated by reverting the change -- it passes on
// master by construction. It was validated with the inverse mutant instead:
// extending the exemption to droppedColumnSubjects turns the first row into
// []string(nil) and leaves the second row green.
func TestAnalyzeFS_DropColumnStillReportsOnASameFileCreatedTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "table created in the same file",
			sql:  "CREATE TABLE users (id int, nick text);\nALTER TABLE users DROP COLUMN nick;",
			want: []string{"DS102"},
		},
		{
			name: "table not created in this file",
			sql:  "ALTER TABLE users DROP COLUMN nick;",
			want: []string{"DS102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native, atlas := analyzeRename(c, map[string]string{"1_drop.sql": test.sql + "\n"})
			c.Assert(rulesOf(native), qt.DeepEquals, test.want)
			c.Assert(rulesOf(atlas), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_DropColumnKeepsEveryDroppedColumnInOneFinding is the second
// non-interference control for the same restructure: a multi-clause DROP COLUMN
// is one finding carrying every column, which is the shape the compatibility
// renderer turns into the plural "Dropping non-virtual columns" wording landed
// for part 2. Splitting it into one finding per column while moving the rule to
// file scope would have silently changed that sentence.
//
// Validated with an inverse mutant -- emitting one finding per subject makes the
// assertion read []string{"DS102", "DS102"} with one subject each.
func TestAnalyzeFS_DropColumnKeepsEveryDroppedColumnInOneFinding(t *testing.T) {
	c := qt.New(t)

	native, _ := analyzeRename(c, map[string]string{
		"1_drop.sql": "ALTER TABLE users DROP COLUMN nick, DROP COLUMN email;\n",
	})

	c.Assert(rulesOf(native), qt.DeepEquals, []string{"DS102"})
	c.Assert(subjectsOf(native), qt.DeepEquals, [][]lint.Subject{{
		{Kind: lint.SubjectColumn, Name: "nick", Parent: "users"},
		{Kind: lint.SubjectColumn, Name: "email", Parent: "users"},
	}})
}

// TestAnalyzeFS_TableRenameIsTwoSchemaChanges pins the semantic change count a
// rename contributes, measured: one `ALTER TABLE t RENAME TO u` counts as two
// schema changes and two such statements as four, while one `ALTER TABLE t
// RENAME COLUMN a TO b` counts as one. A table rename retires one object name
// and introduces another; a column rename modifies a table that stays the same
// object.
//
// The column row is what separates "renames count double" from "table renames
// count double". Reverting the change gives 1 for the first row and 2 for the
// second.
func TestAnalyzeFS_TableRenameIsTwoSchemaChanges(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []lint.SchemaChange
	}{
		{
			name: "table rename",
			sql:  "ALTER TABLE users RENAME TO accounts;",
			want: []lint.SchemaChange{
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "accounts"},
			},
		},
		{
			name: "column rename",
			sql:  "ALTER TABLE users RENAME COLUMN id TO oid;",
			want: []lint.SchemaChange{
				{Kind: lint.SchemaChangeRename, Object: "id"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(
				fixture(map[string]string{"1_rename.sql": test.sql + "\n"}),
				lint.Options{DirFormat: migrator.MigrationDirFormatAtlas, Dialect: "postgres"},
			)
			c.Assert(err, qt.IsNil)
			files := analysis.Files()
			c.Assert(files, qt.HasLen, 1)
			kinds := make([]lint.SchemaChange, 0, len(files[0].Changes))
			for _, change := range files[0].Changes {
				kinds = append(kinds, lint.SchemaChange{Kind: change.Kind, Object: change.Object})
			}
			c.Assert(kinds, qt.DeepEquals, test.want)
		})
	}
}

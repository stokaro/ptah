//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A grant on the sequence a serial column owns follows the column's table
// through a selector: `--include items` manages it, `--exclude items` leaves
// it alone, and a selector can name the sequence itself. The sequence is part
// of its column, so neither side of the comparison describes it, and without
// the column's answer an include dropped the grant from both sides while an
// exclusion kept it beside the table it excluded (stokaro/ptah#4063).
//
// Each test reads the plan of a comparison between a PostgreSQL database and a
// schema file that differs from it in that one grant, in either direction: a
// grant the database lacks, and one it holds for a role the file manages.

// columnSequenceGrantDirections are the two ways the file and the database
// differ, keyed by name.
var columnSequenceGrantDirections = map[string]struct {
	// live is granted on the database beside the table grant.
	live string
	// declared is the file's grant section, after CREATE TABLE.
	declared string
	// manageRole makes the file manage the role, so a grant it leaves out is
	// revoked.
	manageRole bool
	// statement is what the plan holds for the sequence when it is managed.
	statement string
}{
	"the database lacks the grant": {
		declared:  "GRANT USAGE ON SEQUENCE items_id_seq TO %[1]s;\n",
		statement: `GRANT USAGE ON SEQUENCE "items_id_seq"`,
	},
	"the database holds an extra grant": {
		live:       "GRANT USAGE ON SEQUENCE items_id_seq TO %[1]s;\n",
		manageRole: true,
		statement:  `REVOKE USAGE ON SEQUENCE "items_id_seq"`,
	},
}

// columnSequenceGrantBinaries run one comparison, with the target and the dev
// database as URLs and the file as a path.
var columnSequenceGrantBinaries = map[string]func(target, dev, file string, selectors []string) (string, error){
	"ptah-compat": func(target, dev, file string, selectors []string) (string, error) {
		args := []string{"schema", "diff", "--from", target, "--to", "file://" + file, "--dev-url", dev}
		return runCompatVerb(append(args, selectors...)...)
	},
	"ptah": func(target, _, file string, selectors []string) (string, error) {
		args := []string{"schema", "diff", "--from", target, "--to", file}
		return runPtahNativeWithError(append(args, selectors...)...)
	},
}

// columnSequenceGrantFixture holds the target and dev databases and the file
// one test compares.
type columnSequenceGrantFixture struct {
	target string
	dev    string
	file   string
}

func newColumnSequenceGrantFixture(c *qt.C, direction string) columnSequenceGrantFixture {
	c.Helper()
	shape := columnSequenceGrantDirections[direction]
	role := fmt.Sprintf("ptah_column_seq_%d", time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	// The role belongs to the server and outlives the databases, which hold
	// grants to it until they are dropped. Registered first, this runs last.
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})

	target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	c.Assert(atlasschema.ApplySQL(c.Context(), target.conn, migrator.MigrationTxModeNone,
		"DROP TABLE kept;\n"+columnSequenceGrantTables+
			fmt.Sprintf("GRANT SELECT ON TABLE items TO %[1]s;\n"+shape.live, role)), qt.IsNil)
	dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone, "DROP TABLE kept;\n"), qt.IsNil)

	var file strings.Builder
	if shape.manageRole {
		fmt.Fprintf(&file, "CREATE ROLE %s;\n", role)
	}
	file.WriteString(columnSequenceGrantTables)
	fmt.Fprintf(&file, "GRANT SELECT ON TABLE items TO %[1]s;\n"+shape.declared, role)
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(file.String()), 0o600), qt.IsNil)
	return columnSequenceGrantFixture{
		target: pinnedDevURL(c, target.url, "public"),
		dev:    pinnedDevURL(c, dev.url, "public"),
		file:   path,
	}
}

const columnSequenceGrantTables = "CREATE TABLE items (id bigserial PRIMARY KEY, title text);\n" +
	"CREATE TABLE other (id int PRIMARY KEY);\n"

// TestSchemaDiffPlansColumnSequenceGrantUnderSelectorE2E compares under the
// selections that keep the sequence's grant: none, the owning table, the
// sequence itself, and an exclusion of another table.
func TestSchemaDiffPlansColumnSequenceGrantUnderSelectorE2E(t *testing.T) {
	tests := []struct {
		name      string
		direction string
		selectors []string
	}{
		{name: "lacks, no selector", direction: "the database lacks the grant"},
		{name: "lacks, include the table", direction: "the database lacks the grant", selectors: []string{"--include", "items"}},
		{
			name:      "lacks, include the sequence",
			direction: "the database lacks the grant",
			selectors: []string{"--include", "items_id_seq"},
		},
		{name: "lacks, exclude another table", direction: "the database lacks the grant", selectors: []string{"--exclude", "other"}},
		{name: "extra, include the table", direction: "the database holds an extra grant", selectors: []string{"--include", "items"}},
		{name: "extra, exclude another table", direction: "the database holds an extra grant", selectors: []string{"--exclude", "other"}},
	}
	for binary, run := range columnSequenceGrantBinaries {
		for _, test := range tests {
			t.Run(binary+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				f := newColumnSequenceGrantFixture(c, test.direction)

				output, err := run(f.target, f.dev, f.file, test.selectors)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Contains, columnSequenceGrantDirections[test.direction].statement)
			})
		}
	}
}

// TestSchemaDiffLeavesColumnSequenceGrantOutsideSelectorE2E compares under the
// selections that leave the sequence's grant alone: another table included,
// and the owning table, its column or the sequence excluded. The plan names
// the sequence nowhere.
func TestSchemaDiffLeavesColumnSequenceGrantOutsideSelectorE2E(t *testing.T) {
	tests := []struct {
		name      string
		direction string
		selectors []string
	}{
		{name: "lacks, include another table", direction: "the database lacks the grant", selectors: []string{"--include", "other"}},
		{name: "lacks, exclude the table", direction: "the database lacks the grant", selectors: []string{"--exclude", "items"}},
		{name: "lacks, exclude the column", direction: "the database lacks the grant", selectors: []string{"--exclude", "items.id"}},
		{
			name:      "lacks, exclude the sequence",
			direction: "the database lacks the grant",
			selectors: []string{"--exclude", "items_id_seq"},
		},
		{name: "extra, include another table", direction: "the database holds an extra grant", selectors: []string{"--include", "other"}},
		{name: "extra, exclude the table", direction: "the database holds an extra grant", selectors: []string{"--exclude", "items"}},
	}
	for binary, run := range columnSequenceGrantBinaries {
		for _, test := range tests {
			t.Run(binary+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				f := newColumnSequenceGrantFixture(c, test.direction)

				output, err := run(f.target, f.dev, f.file, test.selectors)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(output, qt.Not(qt.Contains), "items_id_seq")
			})
		}
	}
}

// TestSchemaDiffExclusionKeepsSequenceOwnedByExcludedTableE2E compares a
// database holding a sequence OWNED BY items.n with a file that leaves the
// sequence out. Unselected, the plan drops it. With items or its column
// excluded, the sequence leaves with its owner, as `--include other` leaves it,
// and the plan names it nowhere.
func TestSchemaDiffExclusionKeepsSequenceOwnedByExcludedTableE2E(t *testing.T) {
	tests := []struct {
		name      string
		selectors []string
		// dropped says whether the plan drops the sequence.
		dropped bool
	}{
		{name: "no selector", dropped: true},
		{name: "exclude another table", selectors: []string{"--exclude", "other"}, dropped: true},
		{name: "include another table", selectors: []string{"--include", "other"}, dropped: false},
		{name: "exclude the owning table", selectors: []string{"--exclude", "items"}, dropped: false},
		{name: "exclude the owning column", selectors: []string{"--exclude", "items.n"}, dropped: false},
	}
	for binary, run := range columnSequenceGrantBinaries {
		for _, test := range tests {
			t.Run(binary+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				tables := "CREATE TABLE items (id int PRIMARY KEY, n int);\nCREATE TABLE other (id int PRIMARY KEY);\n"
				target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
				c.Assert(atlasschema.ApplySQL(c.Context(), target.conn, migrator.MigrationTxModeNone,
					"DROP TABLE kept;\n"+tables+"CREATE SEQUENCE lifecycle_seq OWNED BY items.n;\n"), qt.IsNil)
				dev := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
				c.Assert(atlasschema.ApplySQL(c.Context(), dev.conn, migrator.MigrationTxModeNone, "DROP TABLE kept;\n"), qt.IsNil)
				file := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(file, []byte(tables), 0o600), qt.IsNil)

				output, err := run(pinnedDevURL(c, target.url, "public"), pinnedDevURL(c, dev.url, "public"), file, test.selectors)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(strings.Contains(output, `DROP SEQUENCE IF EXISTS "lifecycle_seq"`), qt.Equals, test.dropped,
					qt.Commentf("%s", output))
			})
		}
	}
}

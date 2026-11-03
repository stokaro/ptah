package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// cutNameSchema declares a foreign key whose name is longer than the 63 bytes
// PostgreSQL keeps, and then names it by those 63 bytes, as a dump of a
// PostgreSQL database does.
const cutNameSchema = `CREATE TABLE groups (id integer PRIMARY KEY);
CREATE TABLE records (
    id integer PRIMARY KEY,
    group_id integer,
    CONSTRAINT "records_groups_breakage_records_group_id_references_the_groups_table" FOREIGN KEY (group_id) REFERENCES groups (id)
);
ALTER TABLE records ALTER CONSTRAINT "records_groups_breakage_records_group_id_references_the_groups_t" DEFERRABLE INITIALLY DEFERRED;
`

func writeCutNameSchema(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(cutNameSchema), 0o600), qt.IsNil)
	return path
}

// TestSchemaExportReadsASQLFileInTheDialectItNames pins that a SQL schema file
// is read in the dialect --dialect names: read for PostgreSQL, the cut name
// resolves to the key the file declares, and the export succeeds.
func TestSchemaExportReadsASQLFileInTheDialectItNames(t *testing.T) {
	c := qt.New(t)
	path := writeCutNameSchema(c)

	stdout, stderr, err := runSchemaExport("--to", "markdown", "--schema-file", path, "--dialect", "postgres")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "records")
}

// TestSchemaExportRefusesWhatItCannotReadInADialect pins the two refusals: a
// file read without a dialect cuts no name, so the cut name names no key, and a
// dialect nothing serves is refused before any file is read.
func TestSchemaExportRefusesWhatItCannotReadInADialect(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "no dialect", args: nil, wantErr: `(?s).*names no foreign key this schema declares by that name.*`},
		{name: "a dialect nothing serves", args: []string{"--dialect", "nosuchdb"}, wantErr: `unsupported --dialect "nosuchdb"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := writeCutNameSchema(c)

			_, _, err := runSchemaExport(append([]string{"--to", "markdown", "--schema-file", path}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

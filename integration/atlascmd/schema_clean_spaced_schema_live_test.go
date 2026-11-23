//go:build integration

package atlas_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// A URL whose search_path names ` a` or `a `, quoted, scopes a clean to that
// schema: the session's current_schema() is the name with its space. The clean keeps the bytes of
// that name, so the plan it prints, and runs when the clean is scoped with
// --include, drops the sequence of ` a` and leaves the sequence of `a` alone.
// Trimmed, the clean read the catalog of `a`: it printed a plan dropping the
// sequence of `a`, and the scoped clean ran it and kept the one it was asked to
// drop.
func TestSchemaCleanSpacedSchema_HappyPath(t *testing.T) {
	commands := []struct {
		name, binary, pkg string
		args              []string
	}{
		{name: "ptah db drop-all", binary: "ptah", pkg: "ptah.run/cmd/ptah", args: []string{"db", "drop-all", "--auto-approve", "--db-url"}},
		{name: "ptah-compat schema clean", binary: "ptah-compat", pkg: "ptah.run/cmd/ptah-compat", args: []string{"schema", "clean", "--auto-approve", "--url"}},
		{
			name: "ptah-compat schema clean --include", binary: "ptah-compat", pkg: "ptah.run/cmd/ptah-compat",
			args: []string{"schema", "clean", "--auto-approve", "--include", "*[type=sequence]", "--url"},
		},
	}
	// spellings put the space before or after the base name.
	spellings := []struct{ name, before, after string }{
		{name: "a leading space", before: " "},
		{name: "a trailing space", after: " "},
	}
	for _, test := range commands {
		for _, spelling := range spellings {
			t.Run(test.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
				base := fmt.Sprintf("ptah_clean_spaced_%d", time.Now().UnixNano())
				spaced := spelling.before + base + spelling.after
				admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() {
					for _, schema := range []string{base, spaced} {
						_, _ = admin.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema))
					}
					dbschema.CloseAndWarn(admin)
				})
				for _, statement := range []string{
					fmt.Sprintf("CREATE SCHEMA %q", base),
					fmt.Sprintf("CREATE SCHEMA %q", spaced),
					fmt.Sprintf("CREATE SEQUENCE %q.keep_seq", base),
					fmt.Sprintf("CREATE SEQUENCE %q.s_space", spaced),
				} {
					_, err = admin.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}
				parsed, err := url.Parse(adminURL)
				c.Assert(err, qt.IsNil)
				// Encode writes a space as `+`, so the value is escaped as a path
				// is, which writes it as %20.
				query := parsed.Query()
				query.Del("search_path")
				parsed.RawQuery = query.Encode() + "&search_path=" + url.PathEscape(fmt.Sprintf("%q", spaced))
				binary := buildSchemaInspectBinary(c, test.binary, test.pkg)

				stdout, stderr, code := runAtlasBinary(binary, nil, append(test.args, parsed.String())...)

				c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
				c.Assert(stdout, qt.Contains, "s_space")
				c.Assert(stdout, qt.Not(qt.Contains), "keep_seq")
				c.Assert(spacedCleanSequences(c, admin, base, spaced), qt.DeepEquals, []string{base + ".keep_seq"})
			})
		}
	}
}

// spacedCleanSequences lists the sequences of schemas as schema.name, in
// order.
func spacedCleanSequences(c *qt.C, conn *dbschema.DatabaseConnection, schemas ...string) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT sequence_schema || '.' || sequence_name FROM information_schema.sequences "+
			"WHERE sequence_schema = ANY($1) ORDER BY 1", schemas)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var sequences []string
	for rows.Next() {
		var sequence string
		c.Assert(rows.Scan(&sequence), qt.IsNil)
		sequences = append(sequences, sequence)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return sequences
}

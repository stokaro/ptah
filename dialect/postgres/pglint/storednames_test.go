package pglint_test

import (
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/dialect/postgres/pglint"
)

// versioned is file with its migration version.
func versioned(version int64, name string, up bool, sql string) lintext.File {
	migration := file(name, up, sql)
	migration.Version = version
	return migration
}

// checkIdentifiers runs PG401P and PG402P over files on target, reported on
// compatibility, and returns where each finding is: its code, its file and its
// statement.
func checkIdentifiers(c *qt.C, target, compatibility string, files ...lintext.File) []string {
	c.Helper()
	request := lintext.CheckRequest{Target: lintext.Target{Name: target}, Codes: []string{"PG401P", "PG402P"}, Files: files,
		Compatibility: compatibility}
	result, err := pglint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateCheck(request, result), qt.IsNil)
	var at []string
	for _, finding := range result.Findings {
		at = append(at, finding.Code+" "+files[finding.File].Name+" "+strconv.Itoa(finding.Statement))
	}
	return at
}

// long is a name of 63 bytes, which PostgreSQL keeps whole and cuts any name
// that starts with it to.
var long = strings.Repeat("k", 63)

// PG401P and PG402P follow the names a directory declares in the order it
// applies them: a name a later file cuts to one an earlier file holds is
// refused, a down file is judged from the names the up files through its
// version leave, and a name an earlier file dropped is free again.
func TestService_CheckMigrations_IdentifierRules(t *testing.T) {
	create := versioned(1, "1.up.sql", true, "CREATE TABLE t (a int, b int);\nCREATE INDEX \""+long+"_alpha\" ON t (a);\n")
	for _, test := range []struct {
		name  string
		files []lintext.File
		want  []string
	}{
		{"a name an earlier file holds", []lintext.File{create,
			versioned(2, "2.up.sql", true, "ALTER TABLE t ADD COLUMN c int;\nCREATE INDEX \""+long+"_beta\" ON t (b);\n")},
			[]string{"PG401P 1.up.sql 1", "PG402P 2.up.sql 1"}},
		{"a down file that runs after its up file", []lintext.File{
			versioned(1, "1.down.sql", false, "CREATE INDEX \""+long+"_beta\" ON t (b);\n"), create},
			[]string{"PG402P 1.down.sql 0", "PG401P 1.up.sql 1"}},
		{"a down file judged after the up file of its version", []lintext.File{create,
			versioned(2, "2.down.sql", false, "CREATE INDEX \""+long+"_gamma\" ON t (b);\n"),
			versioned(2, "2.up.sql", true, "DROP INDEX \""+long+"\";\n")},
			[]string{"PG401P 1.up.sql 1", "PG401P 2.down.sql 0"}},
		{"a down file blind to later versions", []lintext.File{
			versioned(1, "1.down.sql", false, "CREATE INDEX \""+long+"_beta\" ON t (b);\n"),
			versioned(1, "1.up.sql", true, "CREATE TABLE t (a int, b int);\n"),
			versioned(2, "2.up.sql", true, "CREATE INDEX \""+long+"_alpha\" ON t (a);\n")},
			[]string{"PG401P 1.down.sql 0", "PG401P 2.up.sql 0"}},
		{"a name dropped first", []lintext.File{create,
			versioned(2, "2.up.sql", true, "DROP INDEX \""+long+"\";\nCREATE INDEX \""+long+"_beta\" ON t (b);\n")},
			[]string{"PG401P 1.up.sql 1", "PG401P 2.up.sql 1"}},
		{"a statement the shared grammar does not read", []lintext.File{create,
			versioned(2, "2.up.sql", true, "CREATE INDEX \""+long+"_beta\" ON t USING (b;\n")},
			[]string{"PG401P 1.up.sql 1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkIdentifiers(c, "postgres", "", test.files...), qt.DeepEquals, test.want)
		})
	}
}

// The rules report nothing on the compatibility surface, whose `migrate lint`
// keeps the community binary's findings and exit code, and nothing on a target
// whose server keeps long names whole.
func TestService_CheckMigrations_IdentifierRulesStayOut(t *testing.T) {
	files := []lintext.File{versioned(1, "1.up.sql", true, "CREATE TABLE t (a int, b int);\nCREATE INDEX \""+long+"_alpha\" ON t (a);\n"+
		"CREATE INDEX \""+long+"_beta\" ON t (b);\n")}
	for _, test := range []struct {
		name          string
		target        string
		compatibility string
		want          []string
	}{
		{"native PostgreSQL", "postgres", "", []string{"PG401P 1.up.sql 1", "PG402P 1.up.sql 2"}},
		{"the compatibility surface", "postgres", "atlas", nil},
		{"a target that keeps long names", "cockroachdb", "", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkIdentifiers(c, test.target, test.compatibility, files...), qt.DeepEquals, test.want)
		})
	}
}

// What PG401P and PG402P say: the names, the stored name, the limit and the
// remedy, and for a guarded statement the server takes for another, that one
// of the two objects is kept.
func TestService_CheckMigrations_IdentifierMessages(t *testing.T) {
	c := qt.New(t)
	files := []lintext.File{versioned(1, "1.up.sql", true, "CREATE TABLE t (a int, b int);\n"+
		"CREATE INDEX \""+long+"_alpha\" ON t (a);\n"+
		"CREATE INDEX IF NOT EXISTS \""+long+"_beta\" ON t (b);\n"+
		"CREATE INDEX \""+long+"_gamma\" ON t (b);\n")}
	request := lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Codes: []string{"PG401P", "PG402P"}, Files: files}

	result, err := pglint.Service{}.CheckMigrations(c.Context(), request)

	c.Assert(err, qt.IsNil)
	remedy := "; write a name of at most 63 bytes, such as \"" + long + "\", so the migration names what the database holds"
	c.Assert(result.Findings, qt.DeepEquals, []lintext.Finding{
		{Code: "PG401P", Statement: 1, Message: "index \"" + long + "_alpha\" in schema \"public\" is 69 bytes, longer than the 63 bytes " +
			"PostgreSQL keeps; it is stored as \"" + long + "\"" + remedy},
		{Code: "PG401P", Statement: 2, Message: "index \"" + long + "_beta\" in schema \"public\" is 68 bytes, longer than the 63 bytes " +
			"PostgreSQL keeps; it is stored as \"" + long + "\"; index \"" + long + "_alpha\" already holds that name, so PostgreSQL " +
			"keeps one of the two objects" + remedy},
		{Code: "PG402P", Statement: 3, Message: "index \"" + long + "_alpha\" and index \"" + long + "_gamma\" in schema \"public\" " +
			"are both stored as \"" + long + "\", and PostgreSQL refuses the second; rename one of them so the two differ " +
			"within the first 63 bytes"},
	})
}

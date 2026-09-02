package toschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// mysqlSchema parses one MySQL document the way every SQL schema source does.
func mysqlSchema(c *qt.C, sql string) schemamodel.Database {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.MySQL)).Parse()
	c.Assert(err, qt.IsNil)
	database, err := toschema.ToDatabase(statements, platform.MySQL)
	c.Assert(err, qt.IsNil)
	return database
}

// indexNames is the index names a document produced, in order.
func indexNames(database schemamodel.Database) []string {
	names := make([]string, 0, len(database.Indexes))
	for _, index := range database.Indexes {
		names = append(names, index.Name)
	}
	return names
}

// TestToDatabase_UnnamedMySQLIndexNames_HappyPath pins the name a MySQL-family
// server gives an index its author did not name.
//
// Every row was measured against live servers before it was written here, and
// the same rows answer identically on MySQL 8.4.11 and 26.7.0 and on MariaDB
// 11.8.9 and 12.3.3. That matters because the name is not cosmetic: a live
// reader takes index names from the catalog, so a desired model that guesses
// differently from the server never converges, and one that leaves the name
// empty is refused by the comparator outright.
func TestToDatabase_UnnamedMySQLIndexNames_HappyPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "an unnamed index takes its first column's name",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, KEY (email));",
			want: []string{"email"},
		},
		{
			name: "INDEX spells the same thing as KEY",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, INDEX (email));",
			want: []string{"email"},
		},
		{
			name: "a repeated leading column counts upward",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, KEY (a), KEY (a, b), KEY (a, b, id));",
			want: []string{"a", "a_2", "a_3"},
		},
		{
			name: "a name the author wrote is left alone and still claims its place",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, KEY a (b), KEY (a));",
			want: []string{"a", "a_2"},
		},
		{
			name: "the counter skips a suffix somebody already took",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, KEY a_2 (b), KEY (a), KEY (a));",
			want: []string{"a_2", "a", "a_3"},
		},
		{
			name: "a prefix length is not part of the name",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, KEY (email(10)));",
			want: []string{"email"},
		},
		{
			name: "a descending part is not part of the name either",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, KEY (a DESC));",
			want: []string{"a"},
		},
		{
			name: "a column-level UNIQUE claims its column before any index does",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL UNIQUE, KEY (a));",
			want: []string{"a_2"},
		},
		{
			name: "a primary key reserves PRIMARY and not its column",
			sql:  "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, KEY (id));",
			want: []string{"id"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlSchema(c, test.sql)
			c.Assert(indexNames(database), qt.DeepEquals, test.want)
		})
	}
}

// TestToDatabase_UnnamedMySQLUniqueConstraintNames covers the other half of the
// same rule.
//
// An unnamed `UNIQUE KEY (email)` lands in Constraints rather than Indexes and
// so takes a different code path, but the server applies one naming rule to
// both: measured, it becomes an index called `email` with NON_UNIQUE=0. Left
// empty it produced a plan that dropped the constraint the server had named and
// added a nameless one, on every run, without ever converging.
func TestToDatabase_UnnamedMySQLUniqueConstraintNames(t *testing.T) {
	t.Parallel()

	t.Run("an unnamed unique constraint takes its first column's name", func(t *testing.T) {
		c := qt.New(t)
		database := mysqlSchema(c,
			"CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, UNIQUE KEY (email));")
		c.Assert(database.Constraints, qt.HasLen, 1)
		c.Assert(database.Constraints[0].Name, qt.Equals, "email")
		c.Assert(database.Constraints[0].Type, qt.Equals, "UNIQUE")
	})

	t.Run("an unnamed unique constraint claims the name an index would want", func(t *testing.T) {
		c := qt.New(t)
		database := mysqlSchema(c,
			"CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, UNIQUE KEY (a), KEY (a));")
		c.Assert(database.Constraints, qt.HasLen, 1)
		c.Assert(database.Constraints[0].Name, qt.Equals, "a")
		c.Assert(indexNames(database), qt.DeepEquals, []string{"a_2"})
	})
}

// TestToDatabase_TwoUnnamedIndexesBothSurvive is the assertion that the naming
// happens early enough.
//
// schemamodel.Finalize deduplicates indexes on {table, name}, so two unnamed
// indexes on one table share the key {u, ""} and the second one is discarded
// without a word. Naming them anywhere after Finalize -- in the renderer, say
// -- would produce two correct statements for a list that had already lost a
// member. This drives Finalize on purpose rather than stopping at ToDatabase.
func TestToDatabase_TwoUnnamedIndexesBothSurvive(t *testing.T) {
	t.Parallel()
	c := qt.New(t)

	database := mysqlSchema(c,
		"CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, KEY (a), KEY (a, b));")
	schemamodel.Finalize(&database)

	c.Assert(indexNames(database), qt.DeepEquals, []string{"a", "a_2"})
}

// TestToDatabase_UnnamedIndexNamesAreDialectSpecific keeps the rule where it
// belongs.
//
// PostgreSQL derives a different name for the same declaration -- users_a_key
// rather than a -- so applying MySQL's rule to a PostgreSQL document would put
// a name in the desired model that its server never assigns, which is the same
// defect this change removes, pointed at a different engine.
func TestToDatabase_UnnamedIndexNamesAreDialectSpecific(t *testing.T) {
	t.Parallel()
	c := qt.New(t)

	sql := "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, UNIQUE (email));"
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	database, err := toschema.ToDatabase(statements, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Name, qt.Equals, "")
}

// TestToDatabase_UnnamedMySQLIndexNames_FailurePath refuses a document neither
// server would accept.
//
// `KEY (a), KEY a (b)` is answered by both engines with
// `ERROR 1061 (42000): Duplicate key name 'a'`, because the unnamed index takes
// the bare name as soon as it is read and the later explicit one collides with
// it. Accepting it modelled a table neither server can create -- and worse than
// an error, Finalize discarded one of the two silently, so the schema converged
// as though the second had never been written.
func TestToDatabase_UnnamedMySQLIndexNames_FailurePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an explicit name an unnamed index already took",
			sql:     "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, KEY (a), KEY a (b));",
			wantErr: `two indexes on one table claim the same name: a on u`,
		},
		{
			name:    "two explicit names that are the same name",
			sql:     "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, KEY dup (a), KEY dup (b));",
			wantErr: `two indexes on one table claim the same name: dup on u`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.MySQL)).Parse()
			c.Assert(err, qt.IsNil)

			_, err = toschema.ToDatabase(statements, platform.MySQL)

			c.Assert(err, qt.ErrorIs, toschema.ErrDuplicateIndexName)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

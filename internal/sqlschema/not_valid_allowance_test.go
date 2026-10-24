package sqlschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// readDocument reads one SQL document as dialect under ctx, naming it
// source.
func readDocument(ctx context.Context, dialect, source, sql string) (*schemamodel.Database, error) {
	db, _, err := sqlschema.ReadWith(ctx, []byte(sql), dialect, nil,
		sqlschema.Options{Owners: sqlext.None(), Source: source})
	return &db, err
}

// mergedDesired reads each document as PostgreSQL under one collector of
// directives, merges them as a composite desired schema is merged, and
// applies the directives, as a desired-schema loader does.
func mergedDesired(c *qt.C, documents ...string) (*schemamodel.Database, error) {
	c.Helper()
	ctx, allowances := sqlschema.WithNotValidAllowances(context.Background())
	parts := make([]*schemamodel.Database, 0, len(documents))
	for index, document := range documents {
		part, err := readDocument(ctx, platform.Postgres, "part "+"123456789"[index:index+1], document)
		c.Assert(err, qt.IsNil)
		parts = append(parts, part)
	}
	merged, err := schemamodel.Merge(parts...)
	c.Assert(err, qt.IsNil)
	return merged, allowances.Apply(merged)
}

// constraintNamed is the constraint of database with name.
func constraintNamed(c *qt.C, database *schemamodel.Database, name string) schemamodel.Constraint {
	c.Helper()
	for _, constraint := range database.Constraints {
		if constraint.Name == name {
			return constraint
		}
	}
	c.Fatalf("no constraint %q in %v", name, database.Constraints)
	return schemamodel.Constraint{}
}

const declaredKey = `CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint,
  CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) ON DELETE RESTRICT,
  CONSTRAINT items_id_positive CHECK (id > 0));
`

// A directive in another document lets the key that document declares stay
// NOT VALID, read the way the server reads the names, and changes nothing
// else about the key.
func TestApplyNotValidAllowances_HappyPath(t *testing.T) {
	long := "items_" + strings.Repeat("k", 60)
	tests := []struct {
		name       string
		declared   string
		directive  string
		constraint string
	}{
		{"quoted names", declaredKey, `-- ptah:allow-not-valid CONSTRAINT "items_owner_fk" ON "items"`, "items_owner_fk"},
		{"unquoted names fold", declaredKey, `-- ptah:allow-not-valid CONSTRAINT Items_Owner_FK ON Items`, "items_owner_fk"},
		{"a CHECK", declaredKey, `--ptah:allow-not-valid   CONSTRAINT items_id_positive   ON items`, "items_id_positive"},
		{"a qualified table", "CREATE SCHEMA s;\nCREATE TABLE s.p (id int PRIMARY KEY);\n" +
			"CREATE TABLE s.c (id int PRIMARY KEY, p int, CONSTRAINT c_p FOREIGN KEY (p) REFERENCES s.p (id));",
			`-- ptah:allow-not-valid CONSTRAINT c_p ON s.c`, "c_p"},
		{"a name the server cuts", "CREATE TABLE owners (id bigint PRIMARY KEY);\n" +
			"CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint, CONSTRAINT \"" + long + "\" FOREIGN KEY (owner_id) REFERENCES owners (id));",
			`-- ptah:allow-not-valid CONSTRAINT "` + long + `" ON items`, long[:63]},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plain, err := mergedDesired(c, test.declared)
			c.Assert(err, qt.IsNil)

			allowed, err := mergedDesired(c, test.declared, test.directive+"\n")

			c.Assert(err, qt.IsNil)
			got := constraintNamed(c, allowed, test.constraint)
			c.Assert(got.NotValid, qt.IsTrue)
			want := constraintNamed(c, plain, test.constraint)
			want.NotValid = true
			c.Assert(got, qt.DeepEquals, want)
		})
	}
}

// A directive written twice, or in the same document as the key, is the same
// permission.
func TestApplyNotValidAllowances_TheSameDocumentAndTwice(t *testing.T) {
	c := qt.New(t)
	directive := `-- ptah:allow-not-valid CONSTRAINT items_owner_fk ON items` + "\n"

	allowed, err := mergedDesired(c, declaredKey+directive+directive, directive)

	c.Assert(err, qt.IsNil)
	c.Assert(constraintNamed(c, allowed, "items_owner_fk").NotValid, qt.IsTrue)
}

// Text that only looks like a directive -- inside a routine body, or after a
// statement on its line -- is not one: read alone, the document would refuse
// a directive that names nothing it declares.
func TestReadNotValidAllowances_OnlyALineCommentIsADirective(t *testing.T) {
	c := qt.New(t)
	document := declaredKey + `CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$
-- ptah:allow-not-valid CONSTRAINT nothing ON nowhere
SELECT 1
$$;
SELECT 1; -- ptah:allow-not-valid CONSTRAINT nothing ON nowhere
`

	_, err := readDocument(context.Background(), platform.Postgres, "schema.sql", document)

	c.Assert(err, qt.IsNil)
}

// A document read under no collector, as a library caller reads one,
// resolves its directives against itself.
func TestReadWith_ResolvesTheDocumentsOwnDirectivesWithoutACollector(t *testing.T) {
	c := qt.New(t)

	db, err := readDocument(context.Background(), platform.Postgres, "schema.sql",
		declaredKey+"-- ptah:allow-not-valid CONSTRAINT items_owner_fk ON items\n")
	c.Assert(err, qt.IsNil)
	c.Assert(constraintNamed(c, db, "items_owner_fk").NotValid, qt.IsTrue)

	_, err = readDocument(context.Background(), platform.Postgres, "allow.sql",
		"-- ptah:allow-not-valid CONSTRAINT items_owner_fk ON items\n")
	c.Assert(err, qt.ErrorMatches, `ptah:allow-not-valid: allow.sql: line 1: the desired schema declares no constraint "items_owner_fk" on table "items"`)
}

func TestReadNotValidAllowances_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		line    string
		want    string
	}{
		{"a misspelled word", platform.Postgres, `-- ptah:allow-not-vaild CONSTRAINT k ON t`,
			`ptah:allow-not-valid: line 2: unknown directive ptah:allow-not-vaild; the permission is written ptah:allow-not-valid`},
		{"another word of the family", platform.Postgres, `-- ptah:allow-destructive`,
			`ptah:allow-not-valid: line 2: unknown directive ptah:allow-destructive; .*`},
		{"no table", platform.Postgres, `-- ptah:allow-not-valid CONSTRAINT k`,
			`ptah:allow-not-valid: line 2: write ptah:allow-not-valid CONSTRAINT <name> ON \[<schema>.\]<table>`},
		{"a trailing word", platform.Postgres, `-- ptah:allow-not-valid CONSTRAINT k ON t please`,
			`ptah:allow-not-valid: line 2: write ptah:allow-not-valid .*`},
		{"another kind", platform.Postgres, `-- ptah:allow-not-valid INDEX k ON t`,
			`ptah:allow-not-valid: line 2: write ptah:allow-not-valid .*`},
		{"a dialect without NOT VALID", platform.MySQL, `-- ptah:allow-not-valid CONSTRAINT k ON t`,
			`ptah:allow-not-valid: line 2: mysql has no NOT VALID constraints; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := readDocument(context.Background(), test.dialect, "schema.sql", "CREATE TABLE t (a int);\n"+test.line+"\n")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrNotValidAllowance)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A directive that reaches no CHECK or FOREIGN KEY of the merged schema is
// refused with the place it was written, never ignored.
func TestApplyNotValidAllowances_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		want      string
	}{
		{"no such constraint", `-- ptah:allow-not-valid CONSTRAINT items_nothing_fk ON items`,
			`ptah:allow-not-valid: part 2: line 1: the desired schema declares no constraint "items_nothing_fk" on table "items"`},
		{"another table", `-- ptah:allow-not-valid CONSTRAINT items_owner_fk ON owners`,
			`ptah:allow-not-valid: part 2: line 1: the desired schema declares no constraint "items_owner_fk" on table "owners"`},
		{"qualified where the documents are not", `-- ptah:allow-not-valid CONSTRAINT items_owner_fk ON public.items`,
			`ptah:allow-not-valid: part 2: line 1: the desired schema declares no constraint "items_owner_fk" on table "public.items"; ` +
				`it declares "items_owner_fk" on table "items", which the directive names as the documents do`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := mergedDesired(c, declaredKey, test.directive+"\n")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrNotValidAllowance)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// Only a CHECK or a FOREIGN KEY can be NOT VALID, so a directive that names
// another kind of constraint is refused.
func TestApplyNotValidAllowances_RefusesAnotherKindOfConstraint(t *testing.T) {
	tests := []struct{ name, declared, table, constraint, kind string }{
		{"a unique constraint", "ALTER TABLE items ADD CONSTRAINT items_owner_unique UNIQUE (owner_id);", "items", "items_owner_unique", "UNIQUE"},
		{"a primary key", "CREATE TABLE tags (id bigint, CONSTRAINT tags_pk PRIMARY KEY (id));", "tags", "tags_pk", "PRIMARY KEY"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := test.table

			_, err := mergedDesired(c, declaredKey+test.declared+"\n",
				"-- ptah:allow-not-valid CONSTRAINT "+test.constraint+" ON "+table+"\n")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrNotValidAllowance)
			c.Assert(err, qt.ErrorMatches, `ptah:allow-not-valid: part 2: line 1: constraint "`+test.constraint+`" on table "`+table+
				`" is a `+test.kind+`; only a CHECK or a FOREIGN KEY can stay NOT VALID`)
		})
	}
}

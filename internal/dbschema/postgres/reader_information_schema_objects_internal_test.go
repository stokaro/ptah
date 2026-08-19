package postgres

// White-box testing required: the standard-catalog reads are unexported and the
// branches that reach them are capability lookups inside readConstraints,
// readAllViews and ReadSchema. Each half can be right while the reader still
// asks pg_catalog, which is the state this package was in when a live Spanner
// endpoint answered `Aggregate functions with FILTER clauses are not supported`
// (stokaro/ptah#942).

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// informationSchemaConstraintCatalog is what a live Spanner endpoint answers,
// measured through PGAdapter on:
//
//	CREATE TABLE p (id bigint PRIMARY KEY, code text NOT NULL);
//	CREATE TABLE ch (id bigint PRIMARY KEY, pid bigint, n bigint,
//	                 CONSTRAINT n_pos CHECK (n > 0),
//	                 CONSTRAINT fk_ch FOREIGN KEY (pid) REFERENCES p (id));
//
// The CK_IS_NOT_NULL_ rows are the server's own materialization of NOT NULL,
// and the NO ACTION rules are the standard's defaults printed rather than left
// null. Neither was written by the schema.
func informationSchemaConstraintCatalog() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{
			"constraint_name", "table_name", "constraint_type",
			"column_name", "ordinal_position",
			"referenced_table", "referenced_column",
			"delete_rule", "update_rule", "check_clause",
		},
		Rows: [][]driver.Value{
			{"CK_IS_NOT_NULL_ch_id", "ch", "CHECK", nil, nil, nil, nil, nil, nil, "id IS NOT NULL"},
			{"PK_ch", "ch", "PRIMARY KEY", "id", int64(1), nil, nil, nil, nil, nil},
			{"fk_ch", "ch", "FOREIGN KEY", "pid", int64(1), "p", "id", "NO ACTION", "NO ACTION", nil},
			{"n_pos", "ch", "CHECK", nil, nil, nil, nil, nil, nil, "(n > '0'::bigint)"},
			{"cascade_fk", "ch", "FOREIGN KEY", "pid", int64(1), "p", "id", "CASCADE", "NO ACTION", nil},
			// The row that separates the two halves of the filter: it carries
			// the server's generated NAME and a clause that is not a NOT NULL
			// materialization, so the name alone must not drop it.
			{"CK_IS_NOT_NULL_ch_odd", "ch", "CHECK", nil, nil, nil, nil, nil, nil, "(n < '9'::bigint)"},
			// And the mirror of it: a schema is free to declare exactly the
			// clause the server materializes, under a name of its own.
			{"n_present", "ch", "CHECK", nil, nil, nil, nil, nil, nil, "n IS NOT NULL"},
		},
	}
}

func constraintNamed(constraints []types.DBConstraint, name string) *types.DBConstraint {
	for position := range constraints {
		if constraints[position].Name == name {
			return &constraints[position]
		}
	}
	return nil
}

// The SQL-standard catalog answers the same question pg_constraint does, with
// two things in it that no schema wrote.
func TestReadInformationSchemaConstraints(t *testing.T) {
	tests := []struct {
		name       string
		constraint string
		present    bool
		wantType   string
		wantColumn string
		wantDelete string
		wantCheck  string
	}{
		{
			name: "the primary key arrives with its key column", constraint: "PK_ch",
			present: true, wantType: "PRIMARY KEY", wantColumn: "id",
		},
		{
			// NO ACTION is the absence of a rule, printed as a default. Read
			// literally it turns every plain foreign key into one the schema
			// must re-declare, and on Spanner the render then fails outright
			// with `spanner does not support ON UPDATE NO ACTION`.
			name: "a plain foreign key carries no referential rule", constraint: "fk_ch",
			present: true, wantType: "FOREIGN KEY", wantColumn: "pid",
		},
		{
			name: "a declared rule survives", constraint: "cascade_fk",
			present: true, wantType: "FOREIGN KEY", wantColumn: "pid", wantDelete: "CASCADE",
		},
		{
			name: "a declared check survives", constraint: "n_pos",
			present: true, wantType: "CHECK", wantCheck: "(n > '0'::bigint)",
		},
		{
			// Reporting this would give every NOT NULL column a check
			// constraint no schema declared, and the comparator would plan to
			// create one on every other dialect.
			name: "the materialized NOT NULL check is not a constraint", constraint: "CK_IS_NOT_NULL_ch_id",
		},
		{
			// Both halves are required, and these two rows are the reason. The
			// name is a convention a schema is free to collide with, so a
			// filter reading it alone drops a declared constraint...
			name: "a check that only shares the generated name survives", constraint: "CK_IS_NOT_NULL_ch_odd",
			present: true, wantType: "CHECK", wantCheck: "(n < '9'::bigint)",
		},
		{
			// ...and the clause is one a schema is free to write, so a filter
			// reading THAT alone drops a declared constraint too.
			name: "a declared check with the same clause survives", constraint: "n_present",
			present: true, wantType: "CHECK", wantCheck: "n IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			db := dbtest.Open(c, func(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				return informationSchemaConstraintCatalog(), nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.SpannerPostgres())

			constraints, err := reader.readInformationSchemaConstraints("public")
			c.Assert(err, qt.IsNil)

			found := constraintNamed(constraints, tt.constraint)
			c.Assert(found != nil, qt.Equals, tt.present)
			c.Assert(constraintType(found), qt.Equals, tt.wantType)
			c.Assert(constraintColumn(found), qt.Equals, tt.wantColumn)
			c.Assert(pointerText(constraintDelete(found)), qt.Equals, tt.wantDelete)
			c.Assert(pointerText(constraintCheck(found)), qt.Equals, tt.wantCheck)
		})
	}
}

// The default schema is spelled as the empty string here, as every other read
// in this file spells it. Spelling it "public" keyed every constraint
// "public.ch" while the tables keyed "ch", so enhanceTablesWithConstraints
// matched nothing and every primary key was dropped from the rendered schema
// with no error anywhere.
func TestReadInformationSchemaConstraintsSpellTheDefaultSchemaAsTheTablesDo(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(c, func(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		return informationSchemaConstraintCatalog(), nil
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.SpannerPostgres())

	constraints, err := reader.readInformationSchemaConstraints("public")
	c.Assert(err, qt.IsNil)
	c.Assert(len(constraints) > 0, qt.IsTrue)

	for _, constraint := range constraints {
		c.Assert(constraint.Schema, qt.Equals, "")
		c.Assert(constraint.QualifiedTableName(), qt.Equals, constraint.TableName)
	}
}

// Views are not an object kind a preset rules out -- every dialect this reader
// serves has them -- so the choice is which catalog can answer, not whether to
// ask.
func TestReadInformationSchemaViews(t *testing.T) {
	tests := []struct {
		name            string
		checkOption     driver.Value
		wantCheckOption string
	}{
		{name: "a view with no check option reports NONE", checkOption: nil, wantCheckOption: "NONE"},
		{name: "an empty one reports NONE too", checkOption: "", wantCheckOption: "NONE"},
		{name: "a declared one survives", checkOption: "CASCADED", wantCheckOption: "CASCADED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			db := dbtest.Open(c, func(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				return dbtest.QueryResult{
					Columns: []string{"table_name", "view_definition", "check_option"},
					Rows:    [][]driver.Value{{"v_p", "SELECT id FROM p", tt.checkOption}},
				}, nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.SpannerPostgres())

			views, err := reader.readInformationSchemaViews("public")
			c.Assert(err, qt.IsNil)
			c.Assert(views, qt.HasLen, 1)
			c.Assert(views[0].Name, qt.Equals, "v_p")
			c.Assert(views[0].Body, qt.Equals, "SELECT id FROM p")
			c.Assert(views[0].CheckOption, qt.Equals, tt.wantCheckOption)
		})
	}
}

// The branch is the point, on both reads: a preset that HAS pg_catalog must
// keep asking it, or these reads would replace the PostgreSQL ones everywhere
// and drop what the standard catalog cannot express.
func TestConstraintAndViewReadsPickTheCatalogTheServerHas(t *testing.T) {
	tests := []struct {
		name     string
		caps     capability.Capabilities
		standard bool
	}{
		{name: "spanner has no pg_catalog", caps: capability.SpannerPostgres(), standard: true},
		{name: "postgres has it", caps: capability.Postgres17(), standard: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			queries := make([]string, 0)
			db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				queries = append(queries, query)
				return dbtest.QueryResult{}, nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", tt.caps)

			_, _ = reader.readConstraints()
			_, _ = reader.readAllViews()

			asked := strings.Join(queries, "\n")
			// The PostgreSQL path reads information_schema.table_constraints
			// too -- its "basic" half is standard-catalog and only its anchor
			// is pg_constraint -- so the discriminator is the join only the
			// standard read makes.
			c.Assert(strings.Contains(asked, "information_schema.check_constraints"), qt.Equals, tt.standard)
			// Same for views: both paths name information_schema.views, and only
			// the standard read asks it for check_option in one projection.
			c.Assert(strings.Contains(asked, "table_name, view_definition, check_option"), qt.Equals, tt.standard)
			c.Assert(strings.Contains(asked, "pg_constraint"), qt.Equals, !tt.standard)
		})
	}
}

// constraintType, constraintColumn, constraintDelete and constraintCheck read a
// field from a constraint that may be absent, so one assertion covers the row
// that survives and the row that is filtered out.
func constraintType(constraint *types.DBConstraint) string {
	if constraint == nil {
		return ""
	}
	return constraint.Type
}

func constraintColumn(constraint *types.DBConstraint) string {
	if constraint == nil {
		return ""
	}
	return constraint.ColumnName
}

func constraintDelete(constraint *types.DBConstraint) *string {
	if constraint == nil {
		return nil
	}
	return constraint.DeleteRule
}

func constraintCheck(constraint *types.DBConstraint) *string {
	if constraint == nil {
		return nil
	}
	return constraint.CheckClause
}

func pointerText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

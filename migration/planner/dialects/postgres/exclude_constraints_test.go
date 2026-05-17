package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_ConstraintsAdded(t *testing.T) {
	tests := []struct {
		name        string
		diff        *types.SchemaDiff
		generated   *goschema.Database
		expectedSQL []string
	}{
		{
			name: "EXCLUDE constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Booking",
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
						WhereCondition:  "is_active = true",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true);",
			},
		},
		{
			name: "EXCLUDE constraint without WHERE clause",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"unique_locations"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Location",
						Name:            "unique_locations",
						Type:            "EXCLUDE",
						Table:           "locations",
						UsingMethod:     "gist",
						ExcludeElements: "location WITH &&",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE locations ADD CONSTRAINT unique_locations EXCLUDE USING gist (location WITH &&);",
			},
		},
		{
			name: "CHECK constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"positive_price"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			},
		},
		{
			name: "UNIQUE constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"unique_user_email"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email"},
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE users ADD CONSTRAINT unique_user_email UNIQUE (user_id, email);",
			},
		},
		{
			name: "FOREIGN KEY constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"fk_user"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:    "Order",
						Name:          "fk_user",
						Type:          "FOREIGN KEY",
						Table:         "orders",
						Columns:       []string{"user_id"},
						ForeignTable:  "users",
						ForeignColumn: "id",
						OnDelete:      "CASCADE",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;",
			},
		},
		{
			name: "Multiple constraints added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings", "positive_price"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Booking",
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
					},
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&);",
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := postgres.New()
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			// Convert AST nodes to SQL for verification
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			// Remove header comments and split into statements
			lines := strings.Split(sql, "\n")
			var sqlStatements []string
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "--") {
					sqlStatements = append(sqlStatements, line)
				}
			}

			c.Assert(len(sqlStatements), qt.Equals, len(tt.expectedSQL))
			for i, expected := range tt.expectedSQL {
				c.Assert(sqlStatements[i], qt.Equals, expected)
			}
		})
	}
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"old_constraint"},
	}
	generated := &goschema.Database{}

	pl := postgres.New()
	nodes := pl.GenerateMigrationAST(diff, generated)

	// One DO block per removed constraint. The previous implementation emitted
	// four nodes (create/exec/drop/drop) but none of them actually invoked the
	// drop logic, leaving the constraint in place — see commit message for the
	// full incident report. The DO block executes immediately on parse and
	// leaves no temporary functions behind.
	c.Assert(len(nodes), qt.Equals, 1)

	sql, err := renderer.RenderSQL("postgres", nodes[0])
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DO $ptah$")
	c.Assert(sql, qt.Contains, "information_schema.table_constraints")
	c.Assert(sql, qt.Contains, "constraint_name = 'old_constraint'")
	c.Assert(sql, qt.Contains, "ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I")
	c.Assert(sql, qt.Contains, "$ptah$")
	// The DO block MUST end with a semicolon. SplitSQLStatements is what the
	// migrator uses to chop a migration into individual db.Exec calls, and it
	// tokenizes on `;`. A DO block whose dollar-quoted body ends at `$tag$`
	// without a trailing `;` merges with the following statement and Postgres
	// rejects the merged chunk with `syntax error at or near "DO"`.
	c.Assert(strings.HasSuffix(strings.TrimRight(sql, " \t\r\n"), ";"), qt.IsTrue,
		qt.Commentf("rendered DO block must end with a semicolon; got: %s", sql))
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved_MultipleSplitCleanly(t *testing.T) {
	c := qt.New(t)

	// Two consecutive constraint drops must split into two statements through
	// the SQL statement splitter. The regression this guards against is the
	// missing `;` in VisitRawSQL: without it, two DO blocks merge into one
	// chunk and Postgres rejects the second `DO`.
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"first_constraint", "second_constraint"},
	}
	statements := planner.GenerateSchemaDiffSQLStatements(diff, &goschema.Database{}, "postgres")
	c.Assert(len(statements), qt.Equals, 2,
		qt.Commentf("each DO block must end up as its own statement after SQL splitting; got %d statements:\n%s",
			len(statements), strings.Join(statements, "\n---\n")))
	c.Assert(strings.Contains(statements[0], "first_constraint"), qt.IsTrue)
	c.Assert(strings.Contains(statements[1], "second_constraint"), qt.IsTrue)
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved_EscapesSingleQuoteInName(t *testing.T) {
	c := qt.New(t)

	// PostgreSQL allows quoted identifiers with embedded apostrophes; the
	// DO block interpolates the constraint name into five distinct contexts
	// (one SQL literal, one EXECUTE format() argument, two RAISE NOTICE
	// strings, one SQL comment) and every literal substitution must escape.
	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"don't_drop"},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
	c.Assert(len(nodes), qt.Equals, 1)

	sql, err := renderer.RenderSQL("postgres", nodes[0])
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "'don''t_drop'", qt.Commentf("single quote in constraint name must be SQL-escaped"))
	// The bare unescaped form must NOT appear in any SQL string literal.
	c.Assert(strings.Contains(sql, "'don't_drop'"), qt.IsFalse,
		qt.Commentf("unescaped name in a literal would break parsing; got: %s", sql))
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved_RejectsUnsafeName(t *testing.T) {
	// Names containing $ or newlines would either break the surrounding
	// `$ptah$` dollar-quote tag or terminate the leading SQL comment line.
	// Rather than emit a malformed drop (or a silent warning comment that
	// would loop on every subsequent generate run), the planner emits a DO
	// block whose only action is RAISE EXCEPTION — so the migration fails
	// loudly and the operator has to rename the constraint.
	cases := []struct {
		input          string
		expectVisible  string // the escaped name as it should appear in the SQL literal
		mustNotContain []string
	}{
		{
			input:          "foo$ptah$bar",
			expectVisible:  `foo\$ptah\$bar`, // `$` rendered as `\$` so it can't collapse $ptah$
			mustNotContain: []string{"$ptah$bar"},
		},
		{
			input:         "two\nlines",
			expectVisible: `two\nlines`,
		},
		{
			input:         "carriage\rreturn",
			expectVisible: `carriage\rreturn`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{ConstraintsRemoved: []string{tc.input}}
			nodes := postgres.New().GenerateMigrationAST(diff, &goschema.Database{})
			c.Assert(len(nodes), qt.Equals, 1)

			sql, err := renderer.RenderSQL("postgres", nodes[0])
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Contains(sql, "RAISE EXCEPTION"), qt.IsTrue,
				qt.Commentf("planner must emit RAISE EXCEPTION for unsafe names; got: %s", sql))
			c.Assert(strings.Contains(sql, "ALTER TABLE %I DROP CONSTRAINT"), qt.IsFalse,
				qt.Commentf("unsafe name must NOT produce a drop statement; got: %s", sql))
			// The rejected name must appear inside an embedded SQL single-
			// quoted literal (not as a Postgres identifier), so the operator
			// sees a clean exception message.
			c.Assert(strings.Contains(sql, "''"+tc.expectVisible+"''"), qt.IsTrue,
				qt.Commentf("rejected name must appear escaped inside the exception message; got: %s", sql))
			for _, forbidden := range tc.mustNotContain {
				c.Assert(strings.Contains(sql, forbidden), qt.IsFalse,
					qt.Commentf("escaped output must not contain raw substring %q; got: %s", forbidden, sql))
			}
		})
	}
}

package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// TestRenderTemporalKeysFromSQL_FailurePath reads a temporal key of one column
// from a SQL schema file and renders it for PostgreSQL 18. The SQL reader keeps
// what the statement says, so the refusal comes from the renderer, which every
// schema source reaches. Without it each key renders as written. Applied,
// PostgreSQL 18.7 answers "constraint using WITHOUT OVERLAPS needs at least two
// columns" to a primary or unique key of one column, and "unsupported ON DELETE
// action for foreign key constraint using PERIOD" to a PERIOD foreign key that
// cascades, after the statements before it have run (stokaro/ptah#6330).
func TestRenderTemporalKeysFromSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a primary key of one column",
			sql:     `CREATE TABLE p (valid_at daterange, PRIMARY KEY (valid_at WITHOUT OVERLAPS));`,
			wantErr: `table "p" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`,
		},
		{
			name:    "a primary key of one column ALTER TABLE adds",
			sql:     "CREATE TABLE p (valid_at daterange);\nALTER TABLE p ADD CONSTRAINT p_pk PRIMARY KEY (valid_at WITHOUT OVERLAPS);",
			wantErr: `table "p" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`,
		},
		{
			name:    "a unique key of one column",
			sql:     `CREATE TABLE p (valid_at daterange, CONSTRAINT p_uq UNIQUE (valid_at WITHOUT OVERLAPS));`,
			wantErr: `constraint "p_uq" has without_overlaps and fewer than two columns`,
		},
		{
			name: "a foreign key of one column",
			sql: "CREATE TABLE p (id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));\n" +
				"CREATE TABLE c (valid_at daterange, CONSTRAINT c_fk FOREIGN KEY (PERIOD valid_at) REFERENCES p (PERIOD valid_at));",
			wantErr: `constraint "c_fk" has period and needs the same number of columns, at least two, on each side`,
		},
		{
			name: "a foreign key that cascades",
			sql: "CREATE TABLE p (id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));\n" +
				"CREATE TABLE c (pid int, valid_at daterange, CONSTRAINT c_fk FOREIGN KEY (pid, PERIOD valid_at) " +
				"REFERENCES p (id, PERIOD valid_at) ON DELETE CASCADE);",
			wantErr: `constraint "c_fk" has period and ON DELETE CASCADE; a PERIOD foreign key takes only NO ACTION`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")
			c.Assert(err, qt.IsNil)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, "postgres", capability.Postgres18())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorAs, new(*ptaherr.RenderError))
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// TestRenderTemporalKeysFromSQL_HappyPath is the control for the refusals
// above: the same keys over two columns render with their clauses.
func TestRenderTemporalKeysFromSQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	database, _ := must.Must2(sqlschema.Read([]byte(
		"CREATE TABLE p (id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS));\n"+
			"CREATE TABLE c (pid int, valid_at daterange, CONSTRAINT c_uq UNIQUE (pid, valid_at WITHOUT OVERLAPS), "+
			"CONSTRAINT c_fk FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p (id, PERIOD valid_at));"), "postgres"))

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Any(qt.Contains), `PRIMARY KEY ("id", "valid_at" WITHOUT OVERLAPS)`)
	c.Assert(statements, qt.Any(qt.Contains), `CONSTRAINT "c_uq" UNIQUE ("pid", "valid_at" WITHOUT OVERLAPS)`)
	c.Assert(statements, qt.Any(qt.Contains), `FOREIGN KEY ("pid", PERIOD "valid_at") REFERENCES "p"("id", PERIOD "valid_at")`)
}

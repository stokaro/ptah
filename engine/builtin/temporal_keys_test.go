package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// temporalSchema is a room table with a WITHOUT OVERLAPS primary key, and a
// booking table with a WITHOUT OVERLAPS unique constraint and a PERIOD foreign
// key onto the rooms.
func temporalSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{
				StructName: "Room", Name: "rooms",
				PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: true,
			},
			{StructName: "Booking", Name: "bookings"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Room", Name: "id", Type: "INTEGER"},
			{StructName: "Room", Name: "valid_at", Type: "DATERANGE"},
			{StructName: "Booking", Name: "room_id", Type: "INTEGER"},
			{StructName: "Booking", Name: "valid_at", Type: "DATERANGE"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Booking", Table: "bookings", Name: "bookings_uq", Type: "UNIQUE",
				Columns: []string{"room_id", "valid_at"}, WithoutOverlaps: true,
			},
			{
				StructName: "Booking", Table: "bookings", Name: "bookings_room_fk", Type: "FOREIGN KEY",
				Columns: []string{"room_id", "valid_at"}, ForeignTable: "rooms",
				ForeignColumns: []string{"id", "valid_at"}, Period: true,
			},
		},
	}
}

// TestRenderTemporalKeys_HappyPath writes WITHOUT OVERLAPS after the last
// column of a key and PERIOD before the last column of each side of a foreign
// key. PostgreSQL 18.6 built each statement and recorded pg_constraint.conperiod
// for it (stokaro/ptah#6330).
func TestRenderTemporalKeys_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
		temporalSchema(), "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `PRIMARY KEY ("id", "valid_at" WITHOUT OVERLAPS)`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "bookings_uq" UNIQUE ("room_id", "valid_at" WITHOUT OVERLAPS)`)
	c.Assert(sql, qt.Contains,
		`CONSTRAINT "bookings_room_fk" FOREIGN KEY ("room_id", PERIOD "valid_at") REFERENCES "rooms"("id", PERIOD "valid_at")`)
}

// TestRenderTemporalKeys_PlainKeysStayPlain is the control: the same tables
// without the flags render no clause, so the clauses above come from the
// flags.
func TestRenderTemporalKeys_PlainKeysStayPlain(t *testing.T) {
	c := qt.New(t)
	schema := temporalSchema()
	schema.Tables[0].PrimaryKeyWithoutOverlaps = false
	schema.Constraints[0].WithoutOverlaps = false
	schema.Constraints[1].Period = false

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, "postgres", capability.Postgres18())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "OVERLAPS")
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "PERIOD")
}

// TestRenderTemporalKeys_FailurePath refuses a temporal key where the server
// would build a plain one: PostgreSQL 17.11 answers a syntax error at or near
// "WITHOUT", and CockroachDB and YugabyteDB take no such clause. The refusal
// names the capability, not the dialect.
func TestRenderTemporalKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		schema  func() *schemamodel.Database
		wantErr string
	}{
		{
			name: "a primary key on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(),
			schema:  temporalSchema,
			wantErr: `.*an unnamed key of table "rooms" declares a PRIMARY KEY with WITHOUT OVERLAPS, which requires target capability temporal_keys, unavailable on this postgres target`,
		},
		{
			name: "a unique constraint on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(),
			schema: func() *schemamodel.Database {
				schema := temporalSchema()
				schema.Tables[0].PrimaryKeyWithoutOverlaps = false
				schema.Constraints = schema.Constraints[:1]
				return schema
			},
			wantErr: `.*constraint "bookings_uq" declares a UNIQUE with WITHOUT OVERLAPS, which requires target capability temporal_keys, unavailable on this postgres target`,
		},
		{
			name: "a foreign key on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(),
			schema: func() *schemamodel.Database {
				schema := temporalSchema()
				schema.Tables[0].PrimaryKeyWithoutOverlaps = false
				schema.Constraints = schema.Constraints[1:]
				return schema
			},
			wantErr: `.*constraint "bookings_room_fk" declares a FOREIGN KEY with PERIOD, which requires target capability temporal_keys, unavailable on this postgres target`,
		},
		{
			name: "a primary key on CockroachDB", dialect: "cockroachdb", caps: capability.ForDialect("cockroachdb"),
			schema:  temporalSchema,
			wantErr: `.*WITHOUT OVERLAPS, which requires target capability temporal_keys, unavailable on this cockroachdb target`,
		},
		{
			name: "a primary key on YugabyteDB", dialect: "yugabytedb", caps: capability.ForDialect("yugabytedb"),
			schema:  temporalSchema,
			wantErr: `.*WITHOUT OVERLAPS, which requires target capability temporal_keys, unavailable on this yugabytedb target`,
		},
		{
			name: "a primary key on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			schema:  temporalSchema,
			wantErr: `.*WITHOUT OVERLAPS, which requires target capability temporal_keys, unavailable on this mysql target`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema(), test.dialect, test.caps)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// TestRenderTemporalKeys_AddedConstraintNodes refuses the same keys when a plan
// adds them to a table that exists, which reaches the renderer as a node and
// not through a declaration.
func TestRenderTemporalKeys_AddedConstraintNodes(t *testing.T) {
	unique := ast.NewUniqueConstraint("u", "id", "valid_at")
	unique.WithoutOverlaps = true
	foreign := ast.NewForeignKeyConstraint("f", []string{"pid", "valid_at"}, &ast.ForeignKeyRef{
		Table: "p", Columns: []string{"id", "valid_at"}, Period: true,
	})
	alter := func(constraint *ast.ConstraintNode) *ast.AlterTableNode {
		return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}}}
	}

	t.Run("written on PostgreSQL 18", func(t *testing.T) {
		c := qt.New(t)

		sql, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), alter(unique), alter(foreign))

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `ADD CONSTRAINT "u" UNIQUE ("id", "valid_at" WITHOUT OVERLAPS)`)
		c.Assert(sql, qt.Contains, `FOREIGN KEY ("pid", PERIOD "valid_at") REFERENCES "p"("id", PERIOD "valid_at")`)
	})
	t.Run("a unique key refused on PostgreSQL 17", func(t *testing.T) {
		c := qt.New(t)

		_, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres17(), alter(unique))

		c.Assert(err, qt.ErrorMatches, `.*constraint "u" declares a UNIQUE with WITHOUT OVERLAPS.*temporal_keys.*`)
	})
	t.Run("a foreign key refused on PostgreSQL 17", func(t *testing.T) {
		c := qt.New(t)

		_, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres17(), alter(foreign))

		c.Assert(err, qt.ErrorMatches, `.*declares a FOREIGN KEY with PERIOD.*temporal_keys.*`)
	})
}

package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// Ptah routes Cloud Spanner's PostgreSQL interface through the PostgreSQL
// implementation everywhere: platform.IsPostgresFamily says so, the DDL renderer
// hands spanner to the PostgreSQL visitor, dbschema reads it over pgx, and
// sqlutil.Rebind numbers its parameters $1, $2, … The query builder was the one
// subsystem that did not, so it refused spanner outright.
//
// These cases pin the contract that closes that gap: for the whole DML surface,
// spanner renders byte-identically to postgres. Byte-identity alone would be
// satisfied by both sides regressing together, so every case also pins the
// literal SQL and args — a postgres regression fails the literal, a spanner-only
// regression fails the comparison, and each failure names which.
//
// Spanner has no live coverage in this repository (stokaro/ptah#942), so nothing
// here executes: these are rendering contracts, not execution evidence.

type spannerParityCase struct {
	name     string
	render   func(dialect string) (string, []any, error)
	wantSQL  string
	wantArgs []any
}

func spannerParityCases() []spannerParityCase {
	whereID := func() ast.Expression {
		return &ast.Comparison{
			Left:     &ast.ColumnRef{Name: "id"},
			Operator: ast.OpEqual,
			Right:    &ast.BoundValue{Value: int64(7)},
		}
	}
	limit := int64(10)
	offset := int64(5)

	return []spannerParityCase{
		{
			name: "select with full outer join, limit and offset",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderSelect(&ast.SelectStatement{
					Columns:   []ast.ResultColumn{{Qualifier: "u", Name: "id"}, {Qualifier: "o", Name: "total"}},
					From:      "users",
					FromAlias: "u",
					Joins: []ast.JoinClause{{
						Type:  ast.JoinFull,
						Table: "orders",
						Alias: "o",
						On: &ast.Comparison{
							Left:     &ast.ColumnRef{Qualifier: "u", Name: "id"},
							Operator: ast.OpEqual,
							Right:    &ast.ColumnRef{Qualifier: "o", Name: "user_id"},
						},
					}},
					Where: &ast.Comparison{
						Left:     &ast.ColumnRef{Qualifier: "u", Name: "status"},
						Operator: ast.OpEqual,
						Right:    &ast.BoundValue{Value: "paid"},
					},
					Limit:  &limit,
					Offset: &offset,
				}, dialect)
			},
			wantSQL: `SELECT "u"."id", "o"."total" FROM "users" "u" FULL OUTER JOIN "orders" "o" ` +
				`ON "u"."id" = "o"."user_id" WHERE "u"."status" = $1 LIMIT $2 OFFSET $3`,
			wantArgs: []any{"paid", int64(10), int64(5)},
		},
		{
			name: "select with offset and no limit emits a bare OFFSET",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderSelect(&ast.SelectStatement{
					Columns: []ast.ResultColumn{{Name: "id"}},
					From:    "users",
					Offset:  &offset,
				}, dialect)
			},
			wantSQL:  `SELECT "id" FROM "users" OFFSET $1`,
			wantArgs: []any{int64(5)},
		},
		{
			name: "insert with returning",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderInsert(&ast.InsertStatement{
					Table:     "users",
					Columns:   []string{"id", "name"},
					Rows:      [][]ast.Expression{{&ast.BoundValue{Value: int64(1)}, &ast.BoundValue{Value: "alice"}}},
					Returning: []ast.ColumnRef{{Name: "id"}},
				}, dialect)
			},
			wantSQL:  `INSERT INTO "users" ("id", "name") VALUES ($1, $2) RETURNING "id"`,
			wantArgs: []any{int64(1), "alice"},
		},
		{
			name: "update with returning",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderUpdate(&ast.UpdateStatement{
					Table:     "users",
					Set:       []ast.Assignment{{Column: "name", Value: &ast.BoundValue{Value: "bob"}}},
					Where:     whereID(),
					Returning: []ast.ColumnRef{{Name: "id"}, {Name: "name"}},
				}, dialect)
			},
			wantSQL:  `UPDATE "users" SET "name" = $1 WHERE "id" = $2 RETURNING "id", "name"`,
			wantArgs: []any{"bob", int64(7)},
		},
		{
			name: "delete with returning",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderDelete(&ast.DeleteStatement{
					Table:     "users",
					Where:     whereID(),
					Returning: []ast.ColumnRef{{Name: "id"}},
				}, dialect)
			},
			wantSQL:  `DELETE FROM "users" WHERE "id" = $1 RETURNING "id"`,
			wantArgs: []any{int64(7)},
		},
	}
}

// TestSpannerRendersIdenticallyToPostgres pins the Spanner rendering of the whole
// DML surface against the PostgreSQL rendering of the same statement.
//
// Revert the Spanner entry in selectPlaceholderStyle and every case prints
//
//	error: got non-nil error, want nil
//	error message: renderer: SELECT rendering is not supported for dialect "spanner"
//
// Revert only the Spanner half of supportsReturning and the three RETURNING
// cases print `renderer: spanner does not support RETURNING` where the postgres
// side rendered the clause, and the star-column case below prints that same
// message where it wants the star refusal — four subtests, measured. The two
// SELECT cases stay green, which is what separates the placeholder change from
// the RETURNING change.
func TestSpannerRendersIdenticallyToPostgres(t *testing.T) {
	for _, tc := range spannerParityCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			gotSQL, gotArgs, gotErr := tc.render(platform.Spanner)
			refSQL, refArgs, refErr := tc.render(platform.Postgres)

			c.Assert(refErr, qt.IsNil)
			c.Assert(gotErr, qt.IsNil)
			c.Assert(gotSQL, qt.Equals, refSQL)
			c.Assert(gotArgs, qt.DeepEquals, refArgs)
			c.Assert(gotSQL, qt.Equals, tc.wantSQL)
			c.Assert(gotArgs, qt.DeepEquals, tc.wantArgs)
		})
	}
}

// TestSpannerRejectsTheSameStatementsAsPostgres pins the refusals, so "spanner
// renders like postgres" cannot be read as "spanner renders anything". A
// WHERE-less UPDATE and a star RETURNING are rejected on both, with the same
// message; the dialect name in the first message is the normalized one.
//
// Delete the Unconditional guard and the first case prints
// `got "" want "renderer: update without a WHERE clause must be marked
// unconditional"`; teach RETURNING a star column and the second prints the
// rendered `RETURNING *` against the same want.
func TestSpannerRejectsTheSameStatementsAsPostgres(t *testing.T) {
	tests := []struct {
		name    string
		render  func(dialect string) (string, []any, error)
		wantErr string
	}{
		{
			name: "update without a where clause",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderUpdate(&ast.UpdateStatement{
					Table: "users",
					Set:   []ast.Assignment{{Column: "name", Value: &ast.BoundValue{Value: "bob"}}},
				}, dialect)
			},
			wantErr: "renderer: update without a WHERE clause must be marked unconditional",
		},
		{
			name: "returning a star column",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderDelete(&ast.DeleteStatement{
					Table:         "users",
					Unconditional: true,
					Returning:     []ast.ColumnRef{{Name: "*"}},
				}, dialect)
			},
			wantErr: "renderer: RETURNING does not support a star column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, _, spannerErr := tt.render(platform.Spanner)
			_, _, postgresErr := tt.render(platform.Postgres)

			c.Assert(errorText(spannerErr), qt.Equals, tt.wantErr)
			c.Assert(errorText(postgresErr), qt.Equals, tt.wantErr)
		})
	}
}

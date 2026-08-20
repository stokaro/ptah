package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

func insertUsers() *ast.InsertStatement {
	return &ast.InsertStatement{
		Table:   "users",
		Columns: []string{"id", "name"},
		Rows: [][]ast.Expression{
			{&ast.BoundValue{Value: int64(1)}, &ast.BoundValue{Value: "alice"}},
		},
	}
}

func TestRenderInsert_SingleRow(t *testing.T) {
	wantArgs := []any{int64(1), "alice"}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres uses dollar placeholders and double quotes",
			dialect: platform.Postgres,
			wantSQL: `INSERT INTO "users" ("id", "name") VALUES ($1, $2)`,
		},
		{
			name:    "mysql uses question placeholders and backticks",
			dialect: platform.MySQL,
			wantSQL: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)",
		},
		{
			name:    "mariadb matches mysql",
			dialect: platform.MariaDB,
			wantSQL: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)",
		},
		{
			name:    "sqlite uses question placeholders and double quotes",
			dialect: platform.SQLite,
			wantSQL: `INSERT INTO "users" ("id", "name") VALUES (?, ?)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderInsert(insertUsers(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderInsert_MultiRow(t *testing.T) {
	stmt := &ast.InsertStatement{
		Table:   "users",
		Columns: []string{"id", "name"},
		Rows: [][]ast.Expression{
			{&ast.BoundValue{Value: int64(1)}, &ast.BoundValue{Value: "alice"}},
			{&ast.BoundValue{Value: int64(2)}, &ast.BoundValue{Value: "bob"}},
			{&ast.BoundValue{Value: int64(3)}, &ast.BoundValue{Value: "carol"}},
		},
	}
	wantArgs := []any{int64(1), "alice", int64(2), "bob", int64(3), "carol"}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres numbers every value across rows",
			dialect: platform.Postgres,
			wantSQL: `INSERT INTO "users" ("id", "name") VALUES ($1, $2), ($3, $4), ($5, $6)`,
		},
		{
			name:    "mysql repeats question placeholders",
			dialect: platform.MySQL,
			wantSQL: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?), (?, ?), (?, ?)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderInsert(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderInsert_Returning(t *testing.T) {
	stmt := &ast.InsertStatement{
		Table:     "users",
		Columns:   []string{"name"},
		Rows:      [][]ast.Expression{{&ast.BoundValue{Value: "alice"}}},
		Returning: []ast.ColumnRef{{Name: "id"}, {Name: "created_at"}},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres renders RETURNING",
			dialect: platform.Postgres,
			wantSQL: `INSERT INTO "users" ("name") VALUES ($1) RETURNING "id", "created_at"`,
		},
		{
			name:    "sqlite renders RETURNING",
			dialect: platform.SQLite,
			wantSQL: `INSERT INTO "users" ("name") VALUES (?) RETURNING "id", "created_at"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderInsert(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{"alice"})
		})
	}
}

func TestRenderInsert_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *ast.InsertStatement
		dialect     string
		wantErrLike string
	}{
		{
			name:        "nil statement",
			stmt:        nil,
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil insert statement",
		},
		{
			name: "unsupported dialect",
			stmt: insertUsers(),
			// A dialect the renderer has never been taught. ClickHouse stood
			// here until stokaro/ptah#941 taught it, which is why the example
			// is now a name outside platform's set entirely: an example the
			// builder supports asserts nothing.
			dialect:     "oracle",
			wantErrLike: `renderer: INSERT rendering is not supported for dialect "oracle"`,
		},
		{
			name:        "missing table",
			stmt:        &ast.InsertStatement{Columns: []string{"a"}, Rows: [][]ast.Expression{{&ast.BoundValue{Value: 1}}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert statement requires a table",
		},
		{
			name:        "no columns",
			stmt:        &ast.InsertStatement{Table: "t", Rows: [][]ast.Expression{{}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert statement requires at least one column",
		},
		{
			name:        "no rows",
			stmt:        &ast.InsertStatement{Table: "t", Columns: []string{"a"}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert statement requires at least one row",
		},
		{
			name: "ragged row",
			stmt: &ast.InsertStatement{
				Table:   "t",
				Columns: []string{"a", "b"},
				Rows:    [][]ast.Expression{{&ast.BoundValue{Value: 1}}},
			},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert row 1 has 1 values but there are 2 columns",
		},
		{
			name: "empty column name",
			stmt: &ast.InsertStatement{
				Table:   "t",
				Columns: []string{"  "},
				Rows:    [][]ast.Expression{{&ast.BoundValue{Value: 1}}},
			},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert column has an empty name",
		},
		{
			name: "returning on mysql",
			stmt: &ast.InsertStatement{
				Table:     "t",
				Columns:   []string{"a"},
				Rows:      [][]ast.Expression{{&ast.BoundValue{Value: 1}}},
				Returning: []ast.ColumnRef{{Name: "id"}},
			},
			dialect:     platform.MySQL,
			wantErrLike: "renderer: mysql does not support RETURNING",
		},
		{
			name: "returning on mariadb",
			stmt: &ast.InsertStatement{
				Table:     "t",
				Columns:   []string{"a"},
				Rows:      [][]ast.Expression{{&ast.BoundValue{Value: 1}}},
				Returning: []ast.ColumnRef{{Name: "id"}},
			},
			dialect:     platform.MariaDB,
			wantErrLike: "renderer: mariadb does not support RETURNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderInsert(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func updateUser() *ast.UpdateStatement {
	return &ast.UpdateStatement{
		Table: "users",
		Set: []ast.Assignment{
			{Column: "name", Value: &ast.BoundValue{Value: "bob"}},
			{Column: "email", Value: &ast.BoundValue{Value: "bob@example.com"}},
		},
		Where: &ast.Comparison{
			Left:     &ast.ColumnRef{Name: "id"},
			Operator: ast.OpEqual,
			Right:    &ast.BoundValue{Value: int64(7)},
		},
	}
}

func TestRenderUpdate_SetThenWhere(t *testing.T) {
	wantArgs := []any{"bob", "bob@example.com", int64(7)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres binds SET values before the WHERE value",
			dialect: platform.Postgres,
			wantSQL: `UPDATE "users" SET "name" = $1, "email" = $2 WHERE "id" = $3`,
		},
		{
			name:    "mysql binds SET values before the WHERE value",
			dialect: platform.MySQL,
			wantSQL: "UPDATE `users` SET `name` = ?, `email` = ? WHERE `id` = ?",
		},
		{
			name:    "sqlite binds SET values before the WHERE value",
			dialect: platform.SQLite,
			wantSQL: `UPDATE "users" SET "name" = ?, "email" = ? WHERE "id" = ?`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderUpdate(updateUser(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderUpdate_Returning(t *testing.T) {
	stmt := &ast.UpdateStatement{
		Table:     "users",
		Set:       []ast.Assignment{{Column: "name", Value: &ast.BoundValue{Value: "bob"}}},
		Where:     &ast.Comparison{Left: &ast.ColumnRef{Name: "id"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: int64(7)}},
		Returning: []ast.ColumnRef{{Name: "updated_at"}},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres renders RETURNING",
			dialect: platform.Postgres,
			wantSQL: `UPDATE "users" SET "name" = $1 WHERE "id" = $2 RETURNING "updated_at"`,
		},
		{
			name:    "sqlite renders RETURNING",
			dialect: platform.SQLite,
			wantSQL: `UPDATE "users" SET "name" = ? WHERE "id" = ? RETURNING "updated_at"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderUpdate(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{"bob", int64(7)})
		})
	}
}

func TestRenderUpdate_NoWhereGuard(t *testing.T) {
	t.Run("without unconditional the whole-table update is rejected", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.UpdateStatement{
			Table: "users",
			Set:   []ast.Assignment{{Column: "active", Value: &ast.BoundValue{Value: false}}},
		}
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: update without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})

	t.Run("with unconditional the whole-table update renders", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.UpdateStatement{
			Table:         "users",
			Set:           []ast.Assignment{{Column: "active", Value: &ast.BoundValue{Value: false}}},
			Unconditional: true,
		}
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "active" = $1`)
		c.Assert(args, qt.DeepEquals, []any{false})
	})
}

func TestRenderUpdate_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *ast.UpdateStatement
		dialect     string
		wantErrLike string
	}{
		{
			name:        "nil statement",
			stmt:        nil,
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil update statement",
		},
		{
			name:        "missing table",
			stmt:        &ast.UpdateStatement{Set: []ast.Assignment{{Column: "a", Value: &ast.BoundValue{Value: 1}}}, Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: update statement requires a table",
		},
		{
			name:        "empty set",
			stmt:        &ast.UpdateStatement{Table: "t", Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: update statement requires at least one assignment",
		},
		{
			name:        "empty column",
			stmt:        &ast.UpdateStatement{Table: "t", Set: []ast.Assignment{{Column: " ", Value: &ast.BoundValue{Value: 1}}}, Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: assignment has an empty column",
		},
		{
			name:        "nil value",
			stmt:        &ast.UpdateStatement{Table: "t", Set: []ast.Assignment{{Column: "a"}}, Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: assignment has a nil value",
		},
		{
			name: "returning on mysql",
			stmt: &ast.UpdateStatement{
				Table:     "t",
				Set:       []ast.Assignment{{Column: "a", Value: &ast.BoundValue{Value: 1}}},
				Where:     &ast.Comparison{Left: &ast.ColumnRef{Name: "id"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: 1}},
				Returning: []ast.ColumnRef{{Name: "a"}},
			},
			dialect:     platform.MySQL,
			wantErrLike: "renderer: mysql does not support RETURNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderUpdate(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func deleteUser() *ast.DeleteStatement {
	return &ast.DeleteStatement{
		Table: "users",
		Where: &ast.Comparison{
			Left:     &ast.ColumnRef{Name: "id"},
			Operator: ast.OpEqual,
			Right:    &ast.BoundValue{Value: int64(7)},
		},
	}
}

func TestRenderDelete_Where(t *testing.T) {
	wantArgs := []any{int64(7)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres",
			dialect: platform.Postgres,
			wantSQL: `DELETE FROM "users" WHERE "id" = $1`,
		},
		{
			name:    "mysql",
			dialect: platform.MySQL,
			wantSQL: "DELETE FROM `users` WHERE `id` = ?",
		},
		{
			name:    "sqlite",
			dialect: platform.SQLite,
			wantSQL: `DELETE FROM "users" WHERE "id" = ?`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderDelete(deleteUser(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderDelete_NoWhereGuard(t *testing.T) {
	t.Run("without unconditional the whole-table delete is rejected", func(t *testing.T) {
		c := qt.New(t)
		sql, args, err := renderer.RenderDelete(&ast.DeleteStatement{Table: "users"}, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: delete without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})

	t.Run("with unconditional the whole-table delete renders and binds nothing", func(t *testing.T) {
		c := qt.New(t)
		sql, args, err := renderer.RenderDelete(&ast.DeleteStatement{Table: "users", Unconditional: true}, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "users"`)
		c.Assert(args, qt.HasLen, 0)
	})
}

func TestRenderDelete_Returning(t *testing.T) {
	stmt := &ast.DeleteStatement{
		Table:     "users",
		Where:     &ast.Comparison{Left: &ast.ColumnRef{Name: "id"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: int64(7)}},
		Returning: []ast.ColumnRef{{Name: "id"}},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres renders RETURNING",
			dialect: platform.Postgres,
			wantSQL: `DELETE FROM "users" WHERE "id" = $1 RETURNING "id"`,
		},
		{
			name:    "sqlite renders RETURNING",
			dialect: platform.SQLite,
			wantSQL: `DELETE FROM "users" WHERE "id" = ? RETURNING "id"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderDelete(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(7)})
		})
	}
}

func TestRenderDelete_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *ast.DeleteStatement
		dialect     string
		wantErrLike string
	}{
		{
			name:        "nil statement",
			stmt:        nil,
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil delete statement",
		},
		{
			name: "unsupported dialect",
			stmt: deleteUser(),
			// A dialect the renderer has never been taught. ClickHouse stood
			// here until stokaro/ptah#941 taught it, which is why the example
			// is now a name outside platform's set entirely: an example the
			// builder supports asserts nothing.
			dialect:     "oracle",
			wantErrLike: `renderer: DELETE rendering is not supported for dialect "oracle"`,
		},
		{
			name:        "missing table",
			stmt:        &ast.DeleteStatement{Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: delete statement requires a table",
		},
		{
			name: "returning on mysql",
			stmt: &ast.DeleteStatement{
				Table:     "t",
				Where:     &ast.Comparison{Left: &ast.ColumnRef{Name: "id"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: 1}},
				Returning: []ast.ColumnRef{{Name: "id"}},
			},
			dialect:     platform.MySQL,
			wantErrLike: "renderer: mysql does not support RETURNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderDelete(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

// TestRenderWrite_ValuesStayBound feeds a SQL-injection payload through an INSERT
// value, an UPDATE SET value, and a DELETE WHERE value, and asserts each ends up
// as a bound argument — never spliced into the SQL text.
func TestRenderWrite_ValuesStayBound(t *testing.T) {
	payload := `x'); DROP TABLE users; --`

	t.Run("insert value is bound", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.InsertStatement{
			Table:   "users",
			Columns: []string{"name"},
			Rows:    [][]ast.Expression{{&ast.BoundValue{Value: payload}}},
		}
		sql, args, err := renderer.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "users" ("name") VALUES ($1)`)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE")
		c.Assert(args, qt.DeepEquals, []any{payload})
	})

	t.Run("update set value is bound", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.UpdateStatement{
			Table:         "users",
			Set:           []ast.Assignment{{Column: "name", Value: &ast.BoundValue{Value: payload}}},
			Unconditional: true,
		}
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "name" = $1`)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE")
		c.Assert(args, qt.DeepEquals, []any{payload})
	})

	t.Run("delete where value is bound", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.DeleteStatement{
			Table: "users",
			Where: &ast.Comparison{Left: &ast.ColumnRef{Name: "name"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: payload}},
		}
		sql, args, err := renderer.RenderDelete(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "users" WHERE "name" = $1`)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE")
		c.Assert(args, qt.DeepEquals, []any{payload})
	})
}

// TestRenderWrite_IdentifiersAreQuoted feeds a quote-bearing identifier through a
// table name, an INSERT column, and an UPDATE assignment column, and asserts the
// embedded quote is doubled (escaped) so the identifier can never terminate its
// quotes — an identifier stays an identifier.
func TestRenderWrite_IdentifiersAreQuoted(t *testing.T) {
	t.Run("insert column and table are quoted", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.InsertStatement{
			Table:   `us"ers`,
			Columns: []string{`na"me`},
			Rows:    [][]ast.Expression{{&ast.BoundValue{Value: "alice"}}},
		}
		sql, args, err := renderer.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "us""ers" ("na""me") VALUES ($1)`)
		c.Assert(args, qt.DeepEquals, []any{"alice"})
	})

	t.Run("update assignment column is quoted", func(t *testing.T) {
		c := qt.New(t)
		stmt := &ast.UpdateStatement{
			Table:         "users",
			Set:           []ast.Assignment{{Column: `na"me`, Value: &ast.BoundValue{Value: "alice"}}},
			Unconditional: true,
		}
		sql, args, err := renderer.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "na""me" = $1`)
		c.Assert(args, qt.DeepEquals, []any{"alice"})
	})
}

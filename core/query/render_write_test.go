package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
)

func insertUsers() *query.InsertStatement {
	return &query.InsertStatement{
		Table:   "users",
		Columns: []string{"id", "name"},
		Rows: [][]query.Expression{
			{&query.BoundValue{Value: int64(1)}, &query.BoundValue{Value: "alice"}},
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
			sql, args, err := query.RenderInsert(insertUsers(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderInsert_MultiRow(t *testing.T) {
	stmt := &query.InsertStatement{
		Table:   "users",
		Columns: []string{"id", "name"},
		Rows: [][]query.Expression{
			{&query.BoundValue{Value: int64(1)}, &query.BoundValue{Value: "alice"}},
			{&query.BoundValue{Value: int64(2)}, &query.BoundValue{Value: "bob"}},
			{&query.BoundValue{Value: int64(3)}, &query.BoundValue{Value: "carol"}},
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
			sql, args, err := query.RenderInsert(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderInsert_Returning(t *testing.T) {
	stmt := &query.InsertStatement{
		Table:     "users",
		Columns:   []string{"name"},
		Rows:      [][]query.Expression{{&query.BoundValue{Value: "alice"}}},
		Returning: []query.ColumnRef{{Name: "id"}, {Name: "created_at"}},
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
			sql, args, err := query.RenderInsert(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{"alice"})
		})
	}
}

func TestRenderInsert_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *query.InsertStatement
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
			dialect:     "db2",
			wantErrLike: `renderer: INSERT rendering is not supported for dialect "db2"`,
		},
		{
			name:        "missing table",
			stmt:        &query.InsertStatement{Columns: []string{"a"}, Rows: [][]query.Expression{{&query.BoundValue{Value: 1}}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert statement requires a table",
		},
		{
			name:        "no columns",
			stmt:        &query.InsertStatement{Table: "t", Rows: [][]query.Expression{{}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert statement requires at least one column",
		},
		{
			// The message names both sources now that a SELECT can supply the
			// rows; an insert with neither is still refused (stokaro/ptah#941).
			name:        "neither rows nor a select source",
			stmt:        &query.InsertStatement{Table: "t", Columns: []string{"a"}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert statement requires at least one row or a SELECT source",
		},
		{
			name: "ragged row",
			stmt: &query.InsertStatement{
				Table:   "t",
				Columns: []string{"a", "b"},
				Rows:    [][]query.Expression{{&query.BoundValue{Value: 1}}},
			},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert row 1 has 1 values but there are 2 columns",
		},
		{
			name: "empty column name",
			stmt: &query.InsertStatement{
				Table:   "t",
				Columns: []string{"  "},
				Rows:    [][]query.Expression{{&query.BoundValue{Value: 1}}},
			},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: insert column has an empty name",
		},
		{
			name: "returning on mysql",
			stmt: &query.InsertStatement{
				Table:     "t",
				Columns:   []string{"a"},
				Rows:      [][]query.Expression{{&query.BoundValue{Value: 1}}},
				Returning: []query.ColumnRef{{Name: "id"}},
			},
			dialect:     platform.MySQL,
			wantErrLike: "renderer: mysql does not support RETURNING",
		},
		{
			name: "returning on mariadb",
			stmt: &query.InsertStatement{
				Table:     "t",
				Columns:   []string{"a"},
				Rows:      [][]query.Expression{{&query.BoundValue{Value: 1}}},
				Returning: []query.ColumnRef{{Name: "id"}},
			},
			dialect:     platform.MariaDB,
			wantErrLike: "renderer: mariadb does not support RETURNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderInsert(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func updateUser() *query.UpdateStatement {
	return &query.UpdateStatement{
		Table: "users",
		Set: []query.Assignment{
			{Column: "name", Value: &query.BoundValue{Value: "bob"}},
			{Column: "email", Value: &query.BoundValue{Value: "bob@example.com"}},
		},
		Where: &query.Comparison{
			Left:     &query.ColumnRef{Name: "id"},
			Operator: query.OpEqual,
			Right:    &query.BoundValue{Value: int64(7)},
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
			sql, args, err := query.RenderUpdate(updateUser(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderUpdate_Returning(t *testing.T) {
	stmt := &query.UpdateStatement{
		Table:     "users",
		Set:       []query.Assignment{{Column: "name", Value: &query.BoundValue{Value: "bob"}}},
		Where:     &query.Comparison{Left: &query.ColumnRef{Name: "id"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: int64(7)}},
		Returning: []query.ColumnRef{{Name: "updated_at"}},
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
			sql, args, err := query.RenderUpdate(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{"bob", int64(7)})
		})
	}
}

func TestRenderUpdate_NoWhereGuard(t *testing.T) {
	t.Run("without unconditional the whole-table update is rejected", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.UpdateStatement{
			Table: "users",
			Set:   []query.Assignment{{Column: "active", Value: &query.BoundValue{Value: false}}},
		}
		sql, args, err := query.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: update without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})

	t.Run("with unconditional the whole-table update renders", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.UpdateStatement{
			Table:         "users",
			Set:           []query.Assignment{{Column: "active", Value: &query.BoundValue{Value: false}}},
			Unconditional: true,
		}
		sql, args, err := query.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "active" = $1`)
		c.Assert(args, qt.DeepEquals, []any{false})
	})
}

func TestRenderUpdate_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *query.UpdateStatement
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
			stmt:        &query.UpdateStatement{Set: []query.Assignment{{Column: "a", Value: &query.BoundValue{Value: 1}}}, Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: update statement requires a table",
		},
		{
			name:        "empty set",
			stmt:        &query.UpdateStatement{Table: "t", Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: update statement requires at least one assignment",
		},
		{
			name:        "empty column",
			stmt:        &query.UpdateStatement{Table: "t", Set: []query.Assignment{{Column: " ", Value: &query.BoundValue{Value: 1}}}, Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: assignment has an empty column",
		},
		{
			name:        "nil value",
			stmt:        &query.UpdateStatement{Table: "t", Set: []query.Assignment{{Column: "a"}}, Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: assignment has a nil value",
		},
		{
			name: "returning on mysql",
			stmt: &query.UpdateStatement{
				Table:     "t",
				Set:       []query.Assignment{{Column: "a", Value: &query.BoundValue{Value: 1}}},
				Where:     &query.Comparison{Left: &query.ColumnRef{Name: "id"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: 1}},
				Returning: []query.ColumnRef{{Name: "a"}},
			},
			dialect:     platform.MySQL,
			wantErrLike: "renderer: mysql does not support RETURNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderUpdate(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func deleteUser() *query.DeleteStatement {
	return &query.DeleteStatement{
		Table: "users",
		Where: &query.Comparison{
			Left:     &query.ColumnRef{Name: "id"},
			Operator: query.OpEqual,
			Right:    &query.BoundValue{Value: int64(7)},
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
			sql, args, err := query.RenderDelete(deleteUser(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderDelete_NoWhereGuard(t *testing.T) {
	t.Run("without unconditional the whole-table delete is rejected", func(t *testing.T) {
		c := qt.New(t)
		sql, args, err := query.RenderDelete(&query.DeleteStatement{Table: "users"}, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, "renderer: delete without a WHERE clause must be marked unconditional")
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})

	t.Run("with unconditional the whole-table delete renders and binds nothing", func(t *testing.T) {
		c := qt.New(t)
		sql, args, err := query.RenderDelete(&query.DeleteStatement{Table: "users", Unconditional: true}, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `DELETE FROM "users"`)
		c.Assert(args, qt.HasLen, 0)
	})
}

func TestRenderDelete_Returning(t *testing.T) {
	stmt := &query.DeleteStatement{
		Table:     "users",
		Where:     &query.Comparison{Left: &query.ColumnRef{Name: "id"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: int64(7)}},
		Returning: []query.ColumnRef{{Name: "id"}},
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
			sql, args, err := query.RenderDelete(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(7)})
		})
	}
}

func TestRenderDelete_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *query.DeleteStatement
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
			dialect:     "db2",
			wantErrLike: `renderer: DELETE rendering is not supported for dialect "db2"`,
		},
		{
			name:        "missing table",
			stmt:        &query.DeleteStatement{Unconditional: true},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: delete statement requires a table",
		},
		{
			name: "returning on mysql",
			stmt: &query.DeleteStatement{
				Table:     "t",
				Where:     &query.Comparison{Left: &query.ColumnRef{Name: "id"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: 1}},
				Returning: []query.ColumnRef{{Name: "id"}},
			},
			dialect:     platform.MySQL,
			wantErrLike: "renderer: mysql does not support RETURNING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderDelete(tt.stmt, tt.dialect)
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
		stmt := &query.InsertStatement{
			Table:   "users",
			Columns: []string{"name"},
			Rows:    [][]query.Expression{{&query.BoundValue{Value: payload}}},
		}
		sql, args, err := query.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "users" ("name") VALUES ($1)`)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE")
		c.Assert(args, qt.DeepEquals, []any{payload})
	})

	t.Run("update set value is bound", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.UpdateStatement{
			Table:         "users",
			Set:           []query.Assignment{{Column: "name", Value: &query.BoundValue{Value: payload}}},
			Unconditional: true,
		}
		sql, args, err := query.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "name" = $1`)
		c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE")
		c.Assert(args, qt.DeepEquals, []any{payload})
	})

	t.Run("delete where value is bound", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.DeleteStatement{
			Table: "users",
			Where: &query.Comparison{Left: &query.ColumnRef{Name: "name"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: payload}},
		}
		sql, args, err := query.RenderDelete(stmt, platform.Postgres)
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
		stmt := &query.InsertStatement{
			Table:   `us"ers`,
			Columns: []string{`na"me`},
			Rows:    [][]query.Expression{{&query.BoundValue{Value: "alice"}}},
		}
		sql, args, err := query.RenderInsert(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `INSERT INTO "us""ers" ("na""me") VALUES ($1)`)
		c.Assert(args, qt.DeepEquals, []any{"alice"})
	})

	t.Run("update assignment column is quoted", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.UpdateStatement{
			Table:         "users",
			Set:           []query.Assignment{{Column: `na"me`, Value: &query.BoundValue{Value: "alice"}}},
			Unconditional: true,
		}
		sql, args, err := query.RenderUpdate(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `UPDATE "users" SET "na""me" = $1`)
		c.Assert(args, qt.DeepEquals, []any{"alice"})
	})
}

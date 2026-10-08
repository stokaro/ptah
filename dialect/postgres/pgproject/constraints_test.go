package pgproject_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/postgres/pgproject"
)

func keyRequest(kind string, columns []string) schemaprojection.ConstraintRequest {
	before := schemaprojection.TableState{Table: catalog.Table{Name: "items", Schema: "public", Columns: []catalog.Column{
		{Name: "id", DataType: "integer", IsNullable: "NO", NotNullConstraintName: "items_id_not_null"},
		{Name: "label", DataType: "text", IsNullable: "YES"},
		{Name: "payload", DataType: "text", IsNullable: "YES"},
	}}, Indexes: []catalog.Index{{Name: "retained", TableName: "items", Schema: "public", Columns: []string{"label"}, Comment: "keep"}}}
	after := before.Clone()
	after.Constraints = []catalog.Constraint{{Name: "key", TableName: "items", Schema: "public", Type: kind, ColumnNames: columns}}
	return schemaprojection.ConstraintRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres18(),
		Before: before, After: after, Changes: []schemaprojection.ConstraintChange{{After: new(after.Constraints[0].Clone())}}}
}

func TestPostgresConstraintIndexesAndColumnEffects(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		columns []string
		primary bool
		unique  bool
		nulls   *bool
	}{
		{name: "primary", kind: "PRIMARY KEY", columns: []string{"id"}, primary: true},
		{name: "single unique", kind: "UNIQUE", columns: []string{"id"}, unique: true, nulls: new(false)},
		{name: "composite unique", kind: "UNIQUE", columns: []string{"id", "label"}, nulls: new(false)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := keyRequest(test.kind, test.columns)
			request.After.Constraints[0].IncludeColumns = []string{"payload"}
			request.After.Constraints[0].NullsDistinct = test.nulls
			request.Changes[0].After = new(request.After.Constraints[0].Clone())
			result, err := (pgproject.Constraints{}).ProjectConstraints(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Unavailable, qt.Equals, "")
			c.Assert(result.State.Indexes, qt.HasLen, 2)
			c.Assert(result.State.Indexes[0], qt.DeepEquals, request.Before.Indexes[0])
			c.Assert(result.State.Indexes[1].Columns, qt.DeepEquals, test.columns)
			c.Assert(result.State.Indexes[1].Method, qt.Equals, "btree")
			c.Assert(result.State.Indexes[1].IncludeColumns, qt.DeepEquals, []string{"payload"})
			c.Assert(result.State.Indexes[1].NullsDistinct, qt.DeepEquals, test.nulls)
			c.Assert(result.State.Table.Columns[0].IsPrimaryKey, qt.Equals, test.primary)
			c.Assert(result.State.Table.Columns[0].IsUnique, qt.Equals, test.unique)
			c.Assert(request.Before.Indexes, qt.HasLen, 1)
			result.State.Indexes[1].Columns[0] = "mutated"
			c.Assert(request.Changes[0].After.ColumnNames[0], qt.Equals, "id")
		})
	}
}

func TestPostgresPrimaryRemovalKeepsNullabilityAndIndependentIndex(t *testing.T) {
	c := qt.New(t)
	request := keyRequest("PRIMARY KEY", []string{"id"})
	added, err := (pgproject.Constraints{}).ProjectConstraints(t.Context(), request)
	c.Assert(err, qt.IsNil)
	request.Before = added.State.Clone()
	request.After = added.State.Clone()
	request.After.Constraints = nil
	request.Changes = []schemaprojection.ConstraintChange{{Before: new(added.State.Constraints[0].Clone())}}
	removed, err := (pgproject.Constraints{}).ProjectConstraints(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(removed.Unavailable, qt.Equals, "")
	c.Assert(removed.State.Indexes, qt.HasLen, 1)
	c.Assert(removed.State.Indexes[0].Comment, qt.Equals, "keep")
	c.Assert(removed.State.Table.Columns[0].IsPrimaryKey, qt.IsFalse)
	c.Assert(removed.State.Table.Columns[0].IsNullable, qt.Equals, "NO")
	c.Assert(removed.State.Table.Columns[0].NotNullConstraintName, qt.Equals, "items_id_not_null")
}

func TestPostgresForeignKeyHasNoBackingIndex(t *testing.T) {
	c := qt.New(t)
	request := keyRequest("FOREIGN KEY", []string{"id"})
	request.After.Constraints[0].ForeignTable = new("parents")
	request.After.Constraints[0].ForeignColumns = []string{"id"}
	request.Changes[0].After = new(request.After.Constraints[0].Clone())
	result, err := (pgproject.Constraints{}).ProjectConstraints(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Unavailable, qt.Equals, "")
	c.Assert(result.State.Indexes, qt.HasLen, 1)
	c.Assert(result.State.Constraints[0].ForeignSchema, qt.Equals, "public")
	c.Assert(*result.State.Constraints[0].ForeignColumn, qt.Equals, "id")
	c.Assert(*result.State.Constraints[0].DeleteRule, qt.Equals, "NO ACTION")
	c.Assert(*result.State.Constraints[0].UpdateRule, qt.Equals, "NO ACTION")
}

func TestPostgresConstraintProjectionReportsUnavailableState(t *testing.T) {
	cases := []struct {
		name   string
		change func(*schemaprojection.ConstraintRequest)
		want   string
	}{
		{name: "partition descendants", change: func(r *schemaprojection.ConstraintRequest) { r.Before.Table.Partitioned = true }, want: "partition constraint effects"},
		{name: "generated not null name", change: func(r *schemaprojection.ConstraintRequest) {
			r.Before.Table.Columns[0].NotNullConstraintName = ""
			r.After.Table.Columns[0].NotNullConstraintName = ""
		}, want: "server-named NOT NULL"},
		{name: "index conflict", change: func(r *schemaprojection.ConstraintRequest) { r.After.Indexes[0].Name = "key" }, want: "conflicts with an accepted index"},
		{name: "missing key column", change: func(r *schemaprojection.ConstraintRequest) { r.Changes[0].After.ColumnNames = []string{"missing"} }, want: "column is absent"},
		{name: "missing included column", change: func(r *schemaprojection.ConstraintRequest) { r.Changes[0].After.IncludeColumns = []string{"missing"} }, want: "column is absent"},
		{name: "unresolved exclusion", change: func(r *schemaprojection.ConstraintRequest) { r.Changes[0].After.Type = "EXCLUDE" }, want: "exclusion index expressions"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := keyRequest("PRIMARY KEY", []string{"id"})
			test.change(&request)
			result, err := (pgproject.Constraints{}).ProjectConstraints(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.State, qt.IsNil)
			c.Assert(result.Unavailable, qt.Contains, test.want)
		})
	}
}

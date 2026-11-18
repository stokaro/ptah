package descriptor_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// withRevisionTable is a descriptor whose revision table is table.
func withRevisionTable(table descriptor.RevisionTable) descriptor.Dialect {
	described := head("alpha")
	described.RevisionTable = table
	return described
}

func TestNewRegistry_RevisionTable_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table descriptor.RevisionTable
	}{
		{name: "no repeatable update and no query size limit", table: descriptor.RevisionTable{}},
		{name: "a repeatable update with every placeholder", table: descriptor.RevisionTable{
			RepeatableUpdate: "ALTER TABLE {table} UPDATE {assignments} WHERE {where}",
		}},
		{name: "a query size limit with its setting", table: descriptor.RevisionTable{
			QuerySizeLimit:   "SELECT 262144",
			QuerySizeSetting: "max_query_size",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described := withRevisionTable(test.table)

			registry, err := descriptor.NewRegistry(described)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Equal(described), qt.IsTrue)
		})
	}
}

func TestNewRegistry_RevisionTable_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		table descriptor.RevisionTable
		want  string
	}{
		{
			name:  "a repeatable update with no table",
			table: descriptor.RevisionTable{RepeatableUpdate: "UPDATE t SET {assignments} WHERE {where}"},
			want:  `invalid dialect descriptor: dialect "alpha": the repeatable update "UPDATE t SET {assignments} WHERE {where}" has no {table}`,
		},
		{
			name:  "a repeatable update with no assignments",
			table: descriptor.RevisionTable{RepeatableUpdate: "UPDATE {table} SET state = 1 WHERE {where}"},
			want:  `invalid dialect descriptor: dialect "alpha": the repeatable update "UPDATE {table} SET state = 1 WHERE {where}" has no {assignments}`,
		},
		{
			name:  "a repeatable update with no condition",
			table: descriptor.RevisionTable{RepeatableUpdate: "UPDATE {table} SET {assignments}"},
			want:  `invalid dialect descriptor: dialect "alpha": the repeatable update "UPDATE {table} SET {assignments}" has no {where}`,
		},
		{
			name:  "a query size limit with no setting",
			table: descriptor.RevisionTable{QuerySizeLimit: "SELECT 262144"},
			want:  `invalid dialect descriptor: dialect "alpha": a query size limit and the setting it reads are stated together`,
		},
		{
			name:  "a query size setting with no limit",
			table: descriptor.RevisionTable{QuerySizeSetting: "max_query_size"},
			want:  `invalid dialect descriptor: dialect "alpha": a query size limit and the setting it reads are stated together`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(withRevisionTable(test.table))

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.want))
			c.Assert(registry, qt.IsNil)
		})
	}
}

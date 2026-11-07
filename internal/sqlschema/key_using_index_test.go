package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// uniqueKey is what the model keeps of a UNIQUE constraint.
type uniqueKey struct {
	Name           string
	Columns        []string
	IncludeColumns []string
	NullsDistinct  *bool
	Deferrable     bool
	Initially      string
}

// uniqueKeys lists the UNIQUE constraints of database.
func uniqueKeys(database schemamodel.Database) []uniqueKey {
	var keys []uniqueKey
	for _, constraint := range database.Constraints {
		if constraint.Type != "UNIQUE" {
			continue
		}
		keys = append(keys, uniqueKey{
			Name: constraint.Name, Columns: constraint.Columns, IncludeColumns: constraint.IncludeColumns,
			NullsDistinct: constraint.NullsDistinct, Deferrable: constraint.Deferrable, Initially: constraint.Initially,
		})
	}
	return keys
}

// TestReadKeyUsingIndex_UniqueKeyTakesTheIndex pins the UNIQUE key `ALTER
// TABLE ... ADD [CONSTRAINT name] UNIQUE USING INDEX index` leaves, as
// PostgreSQL 18.6 leaves it: the index's columns, INCLUDE columns and NULLS
// treatment, under the constraint's name, or the index's where the statement
// names none, and no index beside it.
func TestReadKeyUsingIndex_UniqueKeyTakesTheIndex(t *testing.T) {
	notDistinct := false
	tests := []struct {
		name string
		sql  string
		want uniqueKey
	}{
		{
			name: "a named key",
			sql:  "CREATE UNIQUE INDEX t_ab ON t (a, b);\nALTER TABLE t ADD CONSTRAINT t_ab_key UNIQUE USING INDEX t_ab;",
			want: uniqueKey{Name: "t_ab_key", Columns: []string{"a", "b"}},
		},
		{
			name: "an unnamed key",
			sql:  "CREATE UNIQUE INDEX t_b ON t (b);\nALTER TABLE t ADD UNIQUE USING INDEX t_b;",
			want: uniqueKey{Name: "t_b", Columns: []string{"b"}},
		},
		{
			name: "an index with INCLUDE columns",
			sql:  "CREATE UNIQUE INDEX t_b ON t (b) INCLUDE (c);\nALTER TABLE t ADD CONSTRAINT t_b_key UNIQUE USING INDEX t_b;",
			want: uniqueKey{Name: "t_b_key", Columns: []string{"b"}, IncludeColumns: []string{"c"}},
		},
		{
			name: "an index whose NULLs are not distinct",
			sql:  "CREATE UNIQUE INDEX t_c ON t (c) NULLS NOT DISTINCT;\nALTER TABLE t ADD CONSTRAINT t_c_key UNIQUE USING INDEX t_c;",
			want: uniqueKey{Name: "t_c_key", Columns: []string{"c"}, NullsDistinct: &notDistinct},
		},
		{
			name: "a deferrable key over an index built concurrently",
			sql: "CREATE UNIQUE INDEX CONCURRENTLY t_a ON t (a);\n" +
				"ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a DEFERRABLE INITIALLY DEFERRED;",
			want: uniqueKey{Name: "t_a_key", Columns: []string{"a"}, Deferrable: true, Initially: "deferred"},
		},
		{
			name: "a quoted index name",
			sql:  "CREATE UNIQUE INDEX \"T_A\" ON t (a);\nALTER TABLE t ADD UNIQUE USING INDEX \"T_A\";",
			want: uniqueKey{Name: "T_A", Columns: []string{"a"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int, b int, c int);\n"+test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(uniqueKeys(database), qt.DeepEquals, []uniqueKey{test.want})
			c.Assert(indexNames(database), qt.HasLen, 0)
		})
	}
}

// TestReadKeyUsingIndex_TheIndexNameIsTheKeys pins the names a key made of an
// index frees and takes, as PostgreSQL 18.6 renames the index: the index's
// name is free again where the constraint names another, and the key's name
// is taken. A file built on the server leaves these indexes.
func TestReadKeyUsingIndex_TheIndexNameIsTheKeys(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantKey     string
		wantIndexes []string
	}{
		{
			name:        "a key named as its index",
			sql:         "ALTER TABLE t ADD CONSTRAINT t_a UNIQUE USING INDEX t_a;",
			wantKey:     "t_a",
			wantIndexes: nil,
		},
		{
			name:        "an index created under the name the key freed",
			sql:         "ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;\nCREATE INDEX t_a ON t (b);",
			wantKey:     "t_a_key",
			wantIndexes: []string{"t_a"},
		},
		{
			name:        "a guarded index under the name the key freed",
			sql:         "ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;\nCREATE INDEX IF NOT EXISTS t_a ON t (b);",
			wantKey:     "t_a_key",
			wantIndexes: []string{"t_a"},
		},
		{
			name:        "a guarded index under the key's name",
			sql:         "ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;\nCREATE INDEX IF NOT EXISTS t_a_key ON t (b);",
			wantKey:     "t_a_key",
			wantIndexes: nil,
		},
		{
			name:        "a guarded index under the name of an unnamed key",
			sql:         "ALTER TABLE t ADD UNIQUE USING INDEX t_a;\nCREATE INDEX IF NOT EXISTS t_a ON t (b);",
			wantKey:     "t_a",
			wantIndexes: nil,
		},
		{
			name:        "a guarded index under a name derived from no columns",
			sql:         "ALTER TABLE t ADD UNIQUE USING INDEX t_a;\nCREATE INDEX IF NOT EXISTS t_key ON t (b);",
			wantKey:     "t_a",
			wantIndexes: []string{"t_key"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int, b int);\nCREATE UNIQUE INDEX t_a ON t (a);\n"+test.sql),
				platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(uniqueKeys(database), qt.DeepEquals, []uniqueKey{{Name: test.wantKey, Columns: []string{"a"}}})
			c.Assert(indexNames(database), qt.CmpEquals(cmpopts.EquateEmpty()), test.wantIndexes)
		})
	}
}

// TestReadKeyUsingIndex_PrimaryKeyTakesTheIndex pins the primary key `ADD
// CONSTRAINT name PRIMARY KEY USING INDEX index` leaves: the table's key,
// over the index's columns and under the constraint's name, and no index.
func TestReadKeyUsingIndex_PrimaryKeyTakesTheIndex(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int, b int, c int);\n"+
		"CREATE UNIQUE INDEX t_ab ON t (a, b) INCLUDE (c);\n"+
		"ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX t_ab;"), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].PrimaryKeyName, qt.Equals, "t_pkey")
	c.Assert(database.Tables[0].PrimaryKey, qt.DeepEquals, []string{"a", "b"})
	c.Assert(database.Tables[0].PrimaryKeyInclude, qt.DeepEquals, []string{"c"})
	c.Assert(indexNames(database), qt.HasLen, 0)
}

// TestReadOntoKeyUsingIndex_TakesAnEarlierFilesIndex pins that a later file of
// a schema directory makes a key of an index an earlier file declared, as the
// server running the files in order does: the index leaves the earlier file's
// model, and the key is the later file's.
func TestReadOntoKeyUsingIndex_TakesAnEarlierFilesIndex(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int);\nCREATE UNIQUE INDEX t_a ON t (a);"), platform.Postgres)
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;"),
		platform.Postgres, sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(earlier), qt.HasLen, 0)
	c.Assert(uniqueKeys(later), qt.DeepEquals, []uniqueKey{{Name: "t_a_key", Columns: []string{"a"}}})
}

// TestReadKeyUsingIndex_FailurePath pins the indexes a key is not made of.
// Each of the first refusals is PostgreSQL 18.6's own, in its words; the rest
// are settings of the index a key keeps nowhere.
func TestReadKeyUsingIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an index that is not unique",
			sql:     "CREATE INDEX t_a ON t (a);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: "t_a" is not a unique index`,
		},
		{
			name:    "a partial index",
			sql:     "CREATE UNIQUE INDEX t_a ON t (a) WHERE a > 0;\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: "t_a" is a partial index`,
		},
		{
			name:    "an index that is not a b-tree",
			sql:     "CREATE UNIQUE INDEX t_a ON t USING hash (a);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" is not a b-tree`,
		},
		{
			name:    "an index over an expression",
			sql:     "CREATE UNIQUE INDEX t_a ON t ((a + 1));\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" contains expressions`,
		},
		{
			name:    "a descending column",
			sql:     "CREATE UNIQUE INDEX t_a ON t (b, a DESC);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" column number 2 does not have default sorting behavior`,
		},
		{
			name:    "a column that sorts NULLs first",
			sql:     "CREATE UNIQUE INDEX t_a ON t (a NULLS FIRST);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" column number 1 does not have default sorting behavior`,
		},
		{
			name: "a primary key over an index whose NULLs are not distinct",
			sql:  "CREATE UNIQUE INDEX t_a ON t (a) NULLS NOT DISTINCT;\nALTER TABLE t ADD PRIMARY KEY USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD PRIMARY KEY USING INDEX t_a: ` +
				`primary keys cannot use NULLS NOT DISTINCT indexes`,
		},
		{
			name: "an operator class",
			sql:  "CREATE UNIQUE INDEX t_a ON t (a int4_ops);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" names operator class int4_ops ` +
				`for column number 1, .*`,
		},
		{
			name:    "storage parameters",
			sql:     "CREATE UNIQUE INDEX t_a ON t (a) WITH (fillfactor = 70);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 3, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" has storage parameters, which a key does not keep; .*`,
		},
		{
			name:    "a comment on the index",
			sql:     "CREATE UNIQUE INDEX t_a ON t (a);\nCOMMENT ON INDEX t_a IS 'x';\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			wantErr: `line 4, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a: index "t_a" has a comment, which a key does not keep; .*`,
		},
		{
			name:    "an index of another table",
			sql:     "CREATE TABLE u (a int);\nCREATE UNIQUE INDEX u_a ON u (a);\nALTER TABLE t ADD UNIQUE USING INDEX u_a;",
			wantErr: `line 4, column 1: ALTER TABLE t ADD UNIQUE USING INDEX u_a names an index this schema does not declare on the table`,
		},
		{
			name: "an index that is already a key",
			sql: "CREATE UNIQUE INDEX t_a ON t (a);\nALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;\n" +
				"ALTER TABLE t ADD CONSTRAINT again UNIQUE USING INDEX t_a_key;",
			wantErr: `line 4, column 1: ALTER TABLE t ADD UNIQUE USING INDEX t_a_key: the index is already the key of a constraint`,
		},
		{
			name:    "a second primary key",
			sql:     "CREATE UNIQUE INDEX t_b ON t (b);\nALTER TABLE t ADD PRIMARY KEY (a);\nALTER TABLE t ADD PRIMARY KEY USING INDEX t_b;",
			wantErr: `line 4, column 1: ALTER TABLE t ADD PRIMARY KEY: the table already declares a primary key`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int, b int, c int);\n"+test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// postsForeignKey is the constraint both #1258 fixtures carry. MySQL reports it
// through information_schema.TABLE_CONSTRAINTS regardless of what the backing
// index ended up being called.
func postsForeignKey() []types.DBConstraint {
	return []types.DBConstraint{
		{
			Name:        "fk_posts_user",
			TableName:   "posts",
			Type:        "FOREIGN KEY",
			ColumnName:  "user_id",
			ColumnNames: []string{"user_id"},
		},
	}
}

// TestIndexes_ForeignKeyBackingIndexIdempotency pins issue #1258 and its
// control together.
//
// Both rows are transcribed from live MySQL 9.7.1. The desired side of each is
// what the pinned community binary's own `schema inspect` emits for that
// database -- it writes the backing index back out as an `index` block in both
// layouts -- and the pinned binary reports "Schema is synced, no changes to be
// made" for both when that output is applied to the database it came from.
//
// The "auto-named" row is the defect: a bare `CONSTRAINT ... FOREIGN KEY` with
// no `KEY` clause makes MySQL name the backing index after the constraint, and
// the database-side filter used to hide it unconditionally, so the declared
// index had nothing to match and was planned as `CREATE INDEX fk_posts_user`.
// MySQL answers that statement with Error 1061, Duplicate key name.
//
// The "distinctly-named" row is the control. It passed before the fix and must
// keep passing: if it ever goes red the suppression has been widened into
// ignoring index drift rather than narrowed.
func TestIndexes_ForeignKeyBackingIndexIdempotency(t *testing.T) {
	tests := []struct {
		name      string
		indexName string
	}{
		{
			// CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(id)
			// -> information_schema.STATISTICS reports index "fk_posts_user".
			name:      "backing index auto-named after the constraint",
			indexName: "fk_posts_user",
		},
		{
			// KEY idx_posts_user (user_id), CONSTRAINT fk_posts_user ...
			// -> MySQL adopts the existing index as the backing index.
			name:      "backing index carries its own name",
			indexName: "idx_posts_user",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Indexes: []goschema.Index{
					{Name: test.indexName, TableName: "posts", Fields: []string{"user_id"}},
				},
			}
			database := &types.DBSchema{
				Constraints: postsForeignKey(),
				Indexes: []types.DBIndex{
					{Name: test.indexName, TableName: "posts", Columns: []string{"user_id"}},
					{Name: "PRIMARY", TableName: "posts", Columns: []string{"id"}, IsPrimary: true, IsUnique: true},
				},
			}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, "mysql")

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// TestIndexes_ForeignKeyBackingIndexDrift is the guard against buying that
// idempotency by ignoring index drift. Every row keeps the foreign key and its
// auto-named backing index in the database and varies only what the desired
// state declares, so each expectation isolates one consequence of narrowing the
// filter.
func TestIndexes_ForeignKeyBackingIndexDrift(t *testing.T) {
	backingIndex := types.DBIndex{
		Name: "fk_posts_user", TableName: "posts", Columns: []string{"user_id"},
	}
	unrelatedIndex := types.DBIndex{
		Name: "idx_posts_created", TableName: "posts", Columns: []string{"created_at"},
	}
	declaredBackingIndex := goschema.Index{
		Name: "fk_posts_user", TableName: "posts", Fields: []string{"user_id"},
	}
	declaredUnrelatedIndex := goschema.Index{
		Name: "idx_posts_created", TableName: "posts", Fields: []string{"created_at"},
	}

	tests := []struct {
		name          string
		generated     []goschema.Index
		database      []types.DBIndex
		wantAdditions []difftypes.IndexRef
		wantRemovals  []difftypes.IndexRef
	}{
		{
			// A genuinely missing index is still reported even though the table
			// also carries a foreign key whose backing index shares its name.
			name:      "index the database lacks is still added",
			generated: []goschema.Index{declaredBackingIndex, declaredUnrelatedIndex},
			database:  []types.DBIndex{backingIndex},
			wantAdditions: []difftypes.IndexRef{
				{Name: "idx_posts_created", TableName: "posts"},
			},
		},
		{
			// The reason the filter exists. A desired state that never mentions
			// the backing index must not plan a DROP INDEX: MySQL refuses to drop
			// the index a live foreign key needs (Error 1553).
			name:      "undeclared backing index is not dropped",
			generated: nil,
			database:  []types.DBIndex{backingIndex},
		},
		{
			// Narrowing the filter must not make an unrelated index immortal.
			name:      "unrelated index the desired state dropped is still removed",
			generated: []goschema.Index{declaredBackingIndex},
			database:  []types.DBIndex{backingIndex, unrelatedIndex},
			wantRemovals: []difftypes.IndexRef{
				{Name: "idx_posts_created", TableName: "posts"},
			},
		},
		{
			// Both halves at once: the backing index the desired state stopped
			// declaring stays hidden, while the index it still declares is
			// compared normally.
			name:      "dropping only the backing index declaration changes nothing",
			generated: []goschema.Index{declaredUnrelatedIndex},
			database:  []types.DBIndex{backingIndex, unrelatedIndex},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Indexes: test.generated}
			database := &types.DBSchema{
				Constraints: postsForeignKey(),
				Indexes:     test.database,
			}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, "mysql")

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, test.wantAdditions)
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, test.wantRemovals)
		})
	}
}

// TestIndexes_ForeignKeyBackingIndexDialects pins which dialects the narrowed
// filter reaches. Only MySQL and MariaDB create a backing index per foreign
// key, so only they populate the filter at all; PostgreSQL never did, and an
// index that happens to share a constraint's name there is an ordinary index.
func TestIndexes_ForeignKeyBackingIndexDialects(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "postgres", dialect: "postgres"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "fk_posts_user", TableName: "posts", Fields: []string{"user_id"}},
				},
			}
			database := &types.DBSchema{
				Constraints: postsForeignKey(),
				Indexes: []types.DBIndex{
					{Name: "fk_posts_user", TableName: "posts", Columns: []string{"user_id"}},
				},
			}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

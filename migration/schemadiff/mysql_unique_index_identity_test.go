package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// mysqlUniqueKeyDatabaseSchema is what MySQL 9.7.1 and MariaDB 11.8.8 report for
//
//	CREATE TABLE users (
//	  id BIGINT NOT NULL AUTO_INCREMENT,
//	  email VARCHAR(255) NOT NULL,
//	  PRIMARY KEY (id),
//	  UNIQUE KEY uq_users_email (email)
//	);
//
// One object appears twice: information_schema.STATISTICS reports the unique
// index and information_schema.TABLE_CONSTRAINTS reports a UNIQUE constraint,
// both named uq_users_email on users.
func mysqlUniqueKeyDatabaseSchema() *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "users",
			Type: "BASE TABLE",
			Columns: []types.DBColumn{{
				Name:       "email",
				DataType:   "varchar(255)",
				ColumnType: "varchar(255)",
				IsNullable: "NO",
				IsUnique:   true,
			}},
		}},
		Indexes: []types.DBIndex{{
			Name:      "uq_users_email",
			TableName: "users",
			Columns:   []string{"email"},
			IsUnique:  true,
		}},
		Constraints: []types.DBConstraint{{
			Name:        "uq_users_email",
			TableName:   "users",
			Type:        "UNIQUE",
			ColumnName:  "email",
			ColumnNames: []string{"email"},
		}},
	}
}

// mysqlUniqueKeyGeneratedSchema is the same table as the desired state writes
// it. `schema inspect` on `ptah-compat` and on the pinned community binary
// v1.3.0, and Ptah's own annotations, all spell MySQL uniqueness as an index,
// so there is no goschema.Constraint at all.
func mysqlUniqueKeyGeneratedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "email",
			Type:       "VARCHAR(255)",
			Nullable:   false,
		}},
		Indexes: []goschema.Index{{
			StructName: "User",
			Name:       "uq_users_email",
			TableName:  "users",
			Fields:     []string{"email"},
			Unique:     true,
		}},
	}
}

// TestCompareWithDialect_MySQLUnchangedUniqueIndexIsSynced is stokaro/ptah#1245
// reduced to the comparison. Measured against MySQL 9.7.1 and MariaDB 11.8.8:
// replaying a database's own `schema inspect` output planned CREATE UNIQUE
// INDEX plus ALTER TABLE DROP INDEX for the same object, where the pinned
// community binary v1.3.0 reported "Schema is synced".
func TestCompareWithDialect_MySQLUnchangedUniqueIndexIsSynced(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				mysqlUniqueKeyGeneratedSchema(),
				mysqlUniqueKeyDatabaseSchema(),
				test.dialect,
			)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_NoObjectIsBothAddedAndRemoved asserts the invariant
// #1245 asks for directly: whatever else a plan does, one name on one table is
// never simultaneously an index addition and a constraint removal. A plan that
// says both cannot be applied -- MySQL answers the CREATE with
// "Error 1061 (42000): Duplicate key name".
func TestCompareWithDialect_NoObjectIsBothAddedAndRemoved(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "postgres", dialect: "postgres"},
		{name: "sqlite", dialect: "sqlite"},
		{name: "sqlserver", dialect: "sqlserver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				mysqlUniqueKeyGeneratedSchema(),
				mysqlUniqueKeyDatabaseSchema(),
				test.dialect,
			)

			c.Assert(
				addedIndexesAlsoRemovedAsConstraints(diff),
				qt.HasLen,
				0,
				qt.Commentf("diff: %+v", diff),
			)
		})
	}
}

// TestCompareWithDialect_NameHeuristicUniqueKeyIsSynced covers the other filter
// that stranded the same object. isConstraintBasedUniqueIndex guesses from the
// name that a unique index backs a constraint, and `uk_users_email` matches its
// MySQL pattern, so the database index was dropped from the pool before the
// same-name UNIQUE constraint was even consulted. Measured on MySQL 9.7.1,
// replaying that database's own inspect output planned CREATE UNIQUE INDEX plus
// ALTER TABLE DROP INDEX where the pinned community binary v1.3.0 reported
// "Schema is synced".
func TestCompareWithDialect_NameHeuristicUniqueKeyIsSynced(t *testing.T) {
	tests := []struct {
		name      string
		indexName string
	}{
		{name: "mysql_uk_prefix", indexName: "uk_users_email"},
		{name: "postgres_key_suffix", indexName: "users_email_key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Indexes[0].Name = test.indexName
			database := mysqlUniqueKeyDatabaseSchema()
			database.Indexes[0].Name = test.indexName
			database.Constraints[0].Name = test.indexName

			diff := schemadiff.CompareWithDialect(generated, database, "mysql")

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_UndeclaredUniqueKeyIsStillRemoved is the control for
// the fix itself: the hand-off to index comparison happens only because the
// desired state declares that index. Declare nothing and the database's UNIQUE
// constraint must still be reported removed, or #1245 would have been "fixed"
// by making constraint removals disappear.
func TestCompareWithDialect_UndeclaredUniqueKeyIsStillRemoved(t *testing.T) {
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
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Indexes = nil

			diff := schemadiff.CompareWithDialect(
				generated,
				mysqlUniqueKeyDatabaseSchema(),
				test.dialect,
			)

			c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{{
				Name:      "uq_users_email",
				TableName: "users",
				Type:      "UNIQUE",
			}})
			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
		})
	}
}

// TestCompareWithDialect_UniqueKeyOnAnotherTableIsNotTheSameObject is the first
// half of the "do not make two different objects equal" control. Index identity
// carries the owning table, so a unique key named uq_users_email on users and
// an index of that name declared on orders are two objects and must stay two.
func TestCompareWithDialect_UniqueKeyOnAnotherTableIsNotTheSameObject(t *testing.T) {
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
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Tables = append(generated.Tables, goschema.Table{
				Name:       "orders",
				StructName: "Order",
			})
			generated.Fields = append(generated.Fields, goschema.Field{
				StructName: "Order",
				Name:       "email",
				Type:       "VARCHAR(255)",
				Nullable:   false,
			})
			generated.Indexes[0].StructName = "Order"
			generated.Indexes[0].TableName = "orders"
			database := mysqlUniqueKeyDatabaseSchema()
			database.Tables = append(database.Tables, types.DBTable{
				Name: "orders",
				Type: "BASE TABLE",
				Columns: []types.DBColumn{{
					Name:       "email",
					DataType:   "varchar(255)",
					ColumnType: "varchar(255)",
					IsNullable: "NO",
					IsUnique:   true,
				}},
			})

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "orders",
			}})
			c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{{
				Name:      "uq_users_email",
				TableName: "users",
				Type:      "UNIQUE",
			}})
		})
	}
}

// TestCompareWithDialect_DeclaredIndexUniquenessStillCompared is the second
// half of that control, and the one the hand-off could have broken. Letting a
// declared identity through means the database's unique key now has a
// counterpart to be matched against; if the match ignored uniqueness, a desired
// plain index would silently satisfy a database UNIQUE KEY of the same name.
// Measured on MySQL 9.7.1, the pinned community binary v1.3.0 plans
// DROP INDEX followed by ADD INDEX for exactly this pair.
func TestCompareWithDialect_DeclaredIndexUniquenessStillCompared(t *testing.T) {
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
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Indexes[0].Unique = false

			diff := schemadiff.CompareWithDialect(
				generated,
				mysqlUniqueKeyDatabaseSchema(),
				test.dialect,
			)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "users",
			}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "users",
			}})
			c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDialect_DeclaredIndexColumnsStillCompared is the same control
// on the other attribute the match could have swallowed. Measured on MySQL
// 9.7.1, the pinned community binary v1.3.0 plans DROP INDEX followed by
// ADD UNIQUE INDEX uq_users_email (name) for this pair.
func TestCompareWithDialect_DeclaredIndexColumnsStillCompared(t *testing.T) {
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
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Fields = append(generated.Fields, goschema.Field{
				StructName: "User",
				Name:       "name",
				Type:       "VARCHAR(255)",
				Nullable:   false,
			})
			generated.Indexes[0].Fields = []string{"name"}
			database := mysqlUniqueKeyDatabaseSchema()
			database.Tables[0].Columns = append(database.Tables[0].Columns, types.DBColumn{
				Name:       "name",
				DataType:   "varchar(255)",
				ColumnType: "varchar(255)",
				IsNullable: "NO",
			})

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "users",
			}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "users",
			}})
			c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDialect_DeclaredUniqueConstraintKeepsConstraintOwnership
// pins the other direction of the ownership rule: a desired state that spells
// uniqueness as a constraint keeps the constraint pool in charge, and the
// database's backing index stays filtered out rather than being reported
// removed.
func TestCompareWithDialect_DeclaredUniqueConstraintKeepsConstraintOwnership(t *testing.T) {
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
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Indexes = nil
			generated.Constraints = []goschema.Constraint{{
				StructName: "User",
				Name:       "uq_users_email",
				Type:       "UNIQUE",
				Table:      "users",
				Columns:    []string{"email"},
			}}

			diff := schemadiff.CompareWithDialect(
				generated,
				mysqlUniqueKeyDatabaseSchema(),
				test.dialect,
			)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_ConstraintBackedIndexRemovalIsRecordedEverywhere
// covers what a removal means when the object it names is a UNIQUE
// constraint's.
//
// The desired state below spells the object as a plain index, so it is a real
// change and index comparison states it as a replacement -- one object, dropped
// and recreated -- on every dialect. The comparator records the fact that a
// UNIQUE constraint enforces the object, not a statement: what removes it
// differs per engine, and what RESTORES it does not. On MySQL and MariaDB the
// unique key and its constraint are one catalog row and `DROP INDEX` removes
// it, which is what the pinned community binary v1.3.0 plans there; on
// PostgreSQL 17.10 that spelling answers `cannot drop index uq_users_email
// because constraint uq_users_email on table users requires it (SQLSTATE
// 2BP01)` and the pinned binary plans `ALTER TABLE "users" DROP CONSTRAINT
// "uq_users_email"`. Both planners read this one list and spell their own drop;
// the down direction reads it on every engine, because a rollback that puts an
// index back where a UNIQUE constraint was restores the wrong object.
func TestCompareWithDialect_ConstraintBackedIndexRemovalIsRecordedEverywhere(t *testing.T) {
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
			generated := mysqlUniqueKeyGeneratedSchema()
			generated.Indexes[0].Unique = false

			diff := schemadiff.CompareWithDialect(
				generated,
				mysqlUniqueKeyDatabaseSchema(),
				test.dialect,
			)

			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "users",
			}})
			c.Assert(diff.ConstraintBackedIndexRemovals, qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "uq_users_email",
				TableName: "users",
			}})
		})
	}
}

// TestCompareWithDialect_PlainIndexRemovalIsNotConstraintBacked is the control
// for the marker: an index no constraint enforces is dropped as an index, on
// PostgreSQL as everywhere else. Marking every removal would turn this one into
// an ALTER TABLE DROP CONSTRAINT for a constraint that does not exist.
func TestCompareWithDialect_PlainIndexRemovalIsNotConstraintBacked(t *testing.T) {

	t.Run("postgres", func(t *testing.T) {
		c := qt.New(t)
		generated := mysqlUniqueKeyGeneratedSchema()
		generated.Indexes = nil
		database := mysqlUniqueKeyDatabaseSchema()
		database.Constraints = nil
		database.Indexes[0].Name = "idx_users_email"
		database.Indexes[0].IsUnique = false

		diff := schemadiff.CompareWithDialect(generated, database, "postgres")

		c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
			Name:      "idx_users_email",
			TableName: "users",
		}})
		c.Assert(diff.ConstraintBackedIndexRemovals, qt.HasLen, 0)
	})
}

// TestCompareWithDialect_MySQLKeyWithAnUnreadablePartDoesNotChurn covers a
// functional key part on MySQL and MariaDB. `KEY idx_mixed (b, (b + 1))` is
// reported by information_schema.STATISTICS as one named column and one row
// whose COLUMN_NAME is NULL, and the reader cannot name the second part -- the
// expression lives in a STATISTICS column MariaDB does not have. Reading what
// it can see as the whole key would find the key short by one part against the
// desired state every run and plan a rebuild forever, on a database MySQL 9.7.1
// and the pinned community binary v1.3.0 both call unchanged. The named part
// here is the desired key's own, so nothing is contradicted and nothing is
// planned.
func TestCompareWithDialect_MySQLKeyWithAnUnreadablePartDoesNotChurn(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				expressionKeyGeneratedSchema(),
				expressionKeyDatabaseSchema(true),
				test.dialect,
			)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_MySQLKeyReadWholeIsStillCompared is the control: the
// declining above is about a key the reader could not read whole, not about
// key columns in general. The same pair with every part named is a genuine
// difference and stays one.
func TestCompareWithDialect_MySQLKeyReadWholeIsStillCompared(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				expressionKeyGeneratedSchema(),
				expressionKeyDatabaseSchema(false),
				test.dialect,
			)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "idx_mixed",
				TableName: "t4",
			}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "idx_mixed",
				TableName: "t4",
			}})
		})
	}
}

// TestCompareWithDialect_MySQLUnreadablePartDoesNotHideANamedDifference is the
// other half of declining a key the reader could not read whole: declining on
// sight of an unreadable part reported two keys as the same key when every part
// the reader COULD read said otherwise.
//
// The database key is `KEY idx_mixed (b, (b + 1))`, read as the one named column
// `b` plus the record of a part that could not be named. The desired key is
// `idx_mixed (c, (c + 1))` -- a different column and a different expression.
// It was reported synchronized. The named part is compared now, so the
// difference is stated as the replacement it is.
func TestCompareWithDialect_MySQLUnreadablePartDoesNotHideANamedDifference(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := expressionKeyGeneratedSchema()
			generated.Fields = append(generated.Fields, goschema.Field{
				StructName: "T4",
				Name:       "c",
				Type:       "INT",
				Nullable:   false,
			})
			generated.Indexes[0].Parts = []goschema.IndexPart{
				{Name: "c"},
				{Expr: "(`c` + 1)"},
			}
			database := expressionKeyDatabaseSchema(true)
			database.Tables[0].Columns = append(database.Tables[0].Columns, types.DBColumn{
				Name:       "c",
				DataType:   "int",
				ColumnType: "int",
				IsNullable: "NO",
			})

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "idx_mixed",
				TableName: "t4",
			}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{
				Name:      "idx_mixed",
				TableName: "t4",
			}})
		})
	}
}

// expressionKeyDatabaseSchema is what MySQL 9.7.1 reports for
//
//	CREATE TABLE t4 (
//	  id BIGINT NOT NULL AUTO_INCREMENT,
//	  b INT NOT NULL,
//	  PRIMARY KEY (id),
//	  KEY idx_mixed (b, (b + 1))
//	);
//
// once the reader has assembled the key: one named column, and -- when
// incomplete is set -- the record that a second part exists which it could not
// name.
func expressionKeyDatabaseSchema(incomplete bool) *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "t4",
			Type: "BASE TABLE",
			Columns: []types.DBColumn{{
				Name:       "b",
				DataType:   "int",
				ColumnType: "int",
				IsNullable: "NO",
			}},
		}},
		Indexes: []types.DBIndex{{
			Name:               "idx_mixed",
			TableName:          "t4",
			Columns:            []string{"b"},
			KeyPartsIncomplete: incomplete,
		}},
	}
}

// expressionKeyGeneratedSchema is the same key as `schema inspect` writes it:
// the named column and the expression, both spelled out.
func expressionKeyGeneratedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "t4", StructName: "T4"}},
		Fields: []goschema.Field{{
			StructName: "T4",
			Name:       "b",
			Type:       "INT",
			Nullable:   false,
		}},
		Indexes: []goschema.Index{{
			StructName: "T4",
			Name:       "idx_mixed",
			TableName:  "t4",
			Parts: []goschema.IndexPart{
				{Name: "b"},
				{Expr: "(`b` + 1)"},
			},
		}},
	}
}

// addedIndexesAlsoRemovedAsConstraints reports every table-qualified name the
// plan creates as an index and drops as a constraint in the same run.
func addedIndexesAlsoRemovedAsConstraints(diff *difftypes.SchemaDiff) []string {
	removed := make(map[difftypes.IndexRef]struct{}, len(diff.ConstraintsRemovedWithTables))
	for _, constraint := range diff.ConstraintsRemovedWithTables {
		removed[difftypes.IndexRef{
			Name:      constraint.Name,
			TableName: constraint.TableName,
		}] = struct{}{}
	}
	collisions := make([]string, 0, len(diff.IndexesAdded))
	for _, index := range diff.IndexesAdded {
		if _, isRemoved := removed[index]; isRemoved {
			collisions = append(collisions, index.TableName+"."+index.Name)
		}
	}
	return collisions
}

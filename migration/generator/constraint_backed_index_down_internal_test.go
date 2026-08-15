package generator

// White-box testing required: these tests drive the unexported up/down
// generation entry points (generateUpMigrationSQL / generateDownMigrationSQL)
// over the real schemadiff comparator, which the exported migration-file API
// does not expose without a filesystem and database connection.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// constraintBackedIndexFixtures is a database whose `uq_users_email` object is a
// UNIQUE constraint -- reported once in the index catalog and once in the
// constraint catalog, which is what every reader on PostgreSQL, MySQL and
// MariaDB does -- against a desired state that names the same object as a PLAIN
// index. That is a real change to one object, and index comparison states it as
// a replacement.
func constraintBackedIndexFixtures() (*goschema.Database, *dbschematypes.DBSchema) {
	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{
			{Name: "id", Type: "BIGINT", StructName: "User", Primary: true, AutoInc: true},
			{Name: "email", Type: "VARCHAR(255)", StructName: "User", Nullable: false},
		},
		Indexes: []goschema.Index{{
			StructName: "User",
			Name:       "uq_users_email",
			TableName:  "users",
			Fields:     []string{"email"},
			Unique:     false,
		}},
	}
	emailLength := 255
	database := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name: "users",
			Type: "BASE TABLE",
			Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
				{
					Name:               "email",
					DataType:           "varchar",
					ColumnType:         "varchar(255)",
					CharacterMaxLength: &emailLength,
					IsNullable:         "NO",
				},
			},
		}},
		Indexes: []dbschematypes.DBIndex{{
			Name:      "uq_users_email",
			TableName: "users",
			Columns:   []string{"email"},
			IsUnique:  true,
		}},
		Constraints: []dbschematypes.DBConstraint{{
			Name:        "uq_users_email",
			TableName:   "users",
			Type:        "UNIQUE",
			ColumnName:  "email",
			ColumnNames: []string{"email"},
		}},
	}
	return generated, database
}

// constraintBackedIndexDialects carries the per-dialect spelling of the same
// three statements. The UP drop differs because the object differs: on
// PostgreSQL the constraint owns the index and the server refuses the index
// spelling (`cannot drop index uq_users_email because constraint uq_users_email
// on table users requires it`, SQLSTATE 2BP01, measured on 17.10), while on
// MySQL and MariaDB a unique key and its constraint are one catalog row that
// DROP INDEX removes, which is what the pinned community binary v1.3.0 plans
// there. The DOWN restore does not differ: ADD CONSTRAINT ... UNIQUE is what
// puts the dropped object back on all three, and on MySQL and MariaDB it lands
// the same catalog row CREATE UNIQUE INDEX would.
var constraintBackedIndexDialects = []struct {
	name    string
	upDrop  string
	create  string
	downDro string
	restore string
}{
	{
		name:    "postgres",
		upDrop:  `ALTER TABLE "users" DROP CONSTRAINT IF EXISTS "uq_users_email"`,
		create:  `CREATE INDEX IF NOT EXISTS "uq_users_email" ON "users" ("email")`,
		downDro: `DROP INDEX IF EXISTS "uq_users_email"`,
		restore: `ALTER TABLE "users" ADD CONSTRAINT "uq_users_email" UNIQUE ("email")`,
	},
	{
		name:    "mysql",
		upDrop:  "DROP INDEX `uq_users_email` ON `users`",
		create:  "CREATE INDEX `uq_users_email` ON `users` (`email`)",
		downDro: "DROP INDEX `uq_users_email` ON `users`",
		restore: "ALTER TABLE `users` ADD CONSTRAINT `uq_users_email` UNIQUE (`email`)",
	},
	{
		name:    "mariadb",
		upDrop:  "DROP INDEX IF EXISTS `uq_users_email` ON `users`",
		create:  "CREATE INDEX `uq_users_email` ON `users` (`email`)",
		downDro: "DROP INDEX IF EXISTS `uq_users_email` ON `users`",
		restore: "ALTER TABLE `users` ADD CONSTRAINT `uq_users_email` UNIQUE (`email`)",
	},
}

// TestGenerateMigration_ConstraintBackedIndexReplacement_DownRestoresTheConstraint
// is the rollback of the #1245 ownership rule.
//
// Letting a declared index own a UNIQUE constraint's object means the plan can
// replace that object, and the down direction has to put back what the up
// direction dropped -- a UNIQUE constraint, not an index. Reversing the removal
// into an index addition did neither: the down direction's target schema comes
// from ConvertDBSchemaToGoSchema, which omits a constraint-backed index because
// the index is the constraint's, so down generation failed outright with
// `invalid schema diff: added index users.uq_users_email at position 0 is
// missing or ambiguous in the target schema`. Measured live on PostgreSQL 17.10
// and MySQL 9.7.1 against a database holding `CONSTRAINT uq_users_email UNIQUE
// (email)`: no migration could be generated at all.
//
// The order in the down file is part of the assertion. ADD CONSTRAINT ... UNIQUE
// builds an index of the constraint's name, so the plain index the up direction
// created has to be dropped first -- PostgreSQL 17.10 answers `relation
// "uq_users_email" already exists` (SQLSTATE 42P07) and MySQL 9.7.1
// `Error 1061 (42000): Duplicate key name 'uq_users_email'` otherwise -- and the
// planners emit constraint additions before index removals.
func TestGenerateMigration_ConstraintBackedIndexReplacement_DownRestoresTheConstraint(t *testing.T) {
	generated, database := constraintBackedIndexFixtures()

	for _, test := range constraintBackedIndexDialects {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(generated, database, test.name)
			c.Assert(diff.ConstraintBackedIndexRemovals, qt.HasLen, 1)

			up, err := generateUpMigrationSQL(diff, generated, test.name)
			c.Assert(err, qt.IsNil)
			assertOrderedPair(c.TB, up, test.upDrop, test.create)

			down, err := generateDownMigrationSQL(diff, generated, database, test.name)
			c.Assert(err, qt.IsNil)
			assertOrderedPair(c.TB, down, test.downDro, test.restore)
		})
	}
}

// TestGenerateMigration_PlainIndexReplacement_DownRebuildsTheIndex is the
// control for the partition: a removal no UNIQUE constraint enforces still
// reverses into an index addition, which is the only thing that restores it.
// Turning every removal into a constraint restoration would emit an
// ADD CONSTRAINT for a constraint the database never had.
func TestGenerateMigration_PlainIndexReplacement_DownRebuildsTheIndex(t *testing.T) {
	generated, database := constraintBackedIndexFixtures()
	generated.Indexes[0].Name = "idx_users_email"
	database.Indexes[0].Name = "idx_users_email"
	database.Indexes[0].IsUnique = true
	database.Constraints = nil

	for _, test := range constraintBackedIndexDialects {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(generated, database, test.name)
			c.Assert(diff.ConstraintBackedIndexRemovals, qt.HasLen, 0)

			down, err := generateDownMigrationSQL(diff, generated, database, test.name)
			c.Assert(err, qt.IsNil)
			c.Assert(down, qt.Not(qt.Contains), "ADD CONSTRAINT")
			c.Assert(strings.Contains(down, "UNIQUE INDEX") ||
				strings.Contains(down, "CREATE UNIQUE"), qt.IsTrue,
				qt.Commentf("down must rebuild the unique index:\n%s", down))
		})
	}
}

func assertOrderedPair(tb testing.TB, sql, first, second string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(strings.Count(sql, first), qt.Equals, 1, qt.Commentf("want one %q in:\n%s", first, sql))
	c.Assert(strings.Count(sql, second), qt.Equals, 1, qt.Commentf("want one %q in:\n%s", second, sql))
	c.Assert(strings.Index(sql, first) < strings.Index(sql, second), qt.IsTrue,
		qt.Commentf("%q must precede %q in:\n%s", first, second, sql))
}

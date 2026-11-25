package generator

// White-box testing required: the exported path that writes a migration file,
// GenerateMigration, needs a live connection, and every engine whose descriptor
// states a rounding condition is a server; SQLite states none. These tests drive
// planGeneratedMigrationSpecs, the offline step that renders the files, with a
// comparison of a PostgreSQL read. The exported path, on each engine, is covered
// by integration/valuerounding.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// roundingDeclaration declares prices.amount as numeric(8,1), and roundingRead is a
// read of the same table holding numeric(8,3): a change that keeps fewer digits after
// the point.
func roundingDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Price", Name: "prices"}},
		Fields: []schemamodel.Field{{StructName: "Price", Name: "amount", Type: "numeric(8,1)", Nullable: true}},
	}
}

func roundingRead() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "prices", Type: "BASE TABLE",
		Columns: []catalog.Column{{Name: "amount", DataType: "numeric(8,3)", ColumnType: "numeric(8,3)", IsNullable: "YES"}},
	}}}
}

func roundingDiff(c *qt.C) *difftypes.SchemaDiff {
	diff, err := schemadiff.CompareWithDialect(c.Context(), roundingDeclaration(), roundingRead(), "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	return diff
}

func planRoundingSpecs(c *qt.C, policy DiffPolicy, qualifier atlasmigrate.Qualifier) generatedMigrationSpec {
	specs, _, err := planGeneratedMigrationSpecs(c.Context(), must.Must(builtin.New()),
		roundingDiff(c), roundingDeclaration(), roundingRead(),
		postgresInfo(capability.Postgres16()), 100, "lower_scale", policy, qualifier,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	return specs[0]
}

const roundingCheckLine = `-- +ptah check kind=rounded-values name="prices.amount" assert="SELECT CASE WHEN EXISTS ` +
	`(SELECT 1 FROM ""prices"" WHERE CAST(""amount"" AS numeric) IS DISTINCT FROM CAST(""amount"" AS numeric(8,1))) ` +
	`THEN 0 ELSE 1 END"`

// TestPlanGeneratedMigrationSpecs_RoundingChecksTheUpFile writes the check the up file
// carries before the statement that converts the column, so the run that applies it
// reads the rows of the database it runs on. The down file widens the column back and
// carries no check.
func TestPlanGeneratedMigrationSpecs_RoundingChecksTheUpFile(t *testing.T) {
	c := qt.New(t)

	spec := planRoundingSpecs(c, DiffPolicy{}, atlasmigrate.Qualifier{})

	c.Assert(spec.UpSQL, qt.Contains, "-- column \"amount\" of table \"prices\" goes from numeric(8,3) to numeric(8,1), "+
		"which keeps fewer digits after the point; the check refuses the migration while a row holds a value the server "+
		"would round or cut\n"+roundingCheckLine+"\n\n")
	alter := strings.Index(spec.UpSQL, `ALTER TABLE "prices" ALTER COLUMN "amount" TYPE`)
	c.Assert(alter > strings.Index(spec.UpSQL, roundingCheckLine), qt.IsTrue, qt.Commentf("%s", spec.UpSQL))
	c.Assert(spec.DownSQL, qt.Not(qt.Contains), "+ptah check")
}

// TestPlanGeneratedMigrationSpecs_AllowedRoundingWritesANote writes, with the rounding
// allowed, a note in place of the check: no check reads the rows when the file runs.
func TestPlanGeneratedMigrationSpecs_AllowedRoundingWritesANote(t *testing.T) {
	c := qt.New(t)

	spec := planRoundingSpecs(c, DiffPolicy{AllowValueRounding: true}, atlasmigrate.Qualifier{})

	c.Assert(spec.UpSQL, qt.Contains, "-- column \"amount\" of table \"prices\" goes from numeric(8,3) to numeric(8,1), "+
		"which keeps fewer digits after the point; the rounding was allowed when this file was generated, so no check "+
		"reads the rows and the server rounds or cuts each value to the new digits\n\n")
	c.Assert(spec.UpSQL, qt.Not(qt.Contains), "+ptah check")
	c.Assert(spec.UpSQL, qt.Contains, `ALTER TABLE "prices" ALTER COLUMN "amount" TYPE`)
}

// TestPlanGeneratedMigrationSpecs_QualifiedRoundingCheckReadsTheQualifiedTable reads the
// table in the schema the qualifier writes into the statements, which is where the file
// converts the column.
func TestPlanGeneratedMigrationSpecs_QualifiedRoundingCheckReadsTheQualifiedTable(t *testing.T) {
	c := qt.New(t)
	qualifier, err := atlasmigrate.ParseQualifier("tenant")
	c.Assert(err, qt.IsNil)

	spec := planRoundingSpecs(c, DiffPolicy{}, qualifier)

	c.Assert(spec.UpSQL, qt.Contains, `-- +ptah check kind=rounded-values name="tenant.prices.amount" assert="SELECT CASE `+
		`WHEN EXISTS (SELECT 1 FROM ""tenant"".""prices"" WHERE`)
	c.Assert(spec.UpSQL, qt.Contains, `ALTER TABLE "tenant"."prices" ALTER COLUMN "amount" TYPE`)
}

// TestPlanGeneratedMigrationSpecs_NoRoundingWritesNoCheck is the control: a change that
// keeps every digit after the point writes neither a check nor a note.
func TestPlanGeneratedMigrationSpecs_NoRoundingWritesNoCheck(t *testing.T) {
	c := qt.New(t)
	desired := roundingDeclaration()
	desired.Fields[0].Type = "numeric(10,3)"
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, roundingRead(), "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)

	specs, _, err := planGeneratedMigrationSpecs(c.Context(), must.Must(builtin.New()),
		diff, desired, roundingRead(), postgresInfo(capability.Postgres16()), 100, "widen", DiffPolicy{}, atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(specs[0].UpSQL, qt.Contains, `ALTER TABLE "prices" ALTER COLUMN "amount" TYPE`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "+ptah check")
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "fewer digits after the point")
}

// planSplitRoundingSpecs plans the rounding change beside a PostgreSQL enum value
// addition, which leads in a file of its own, so the column type change lands in
// the transactional file of a split plan.
func planSplitRoundingSpecs(c *qt.C, policy DiffPolicy) []generatedMigrationSpec {
	diff := roundingDiff(c)
	diff.EnumsModified = []difftypes.EnumDiff{{EnumName: "status", ValuesAdded: []string{"archived"}}}
	desired := roundingDeclaration()
	desired.Enums = []schemamodel.Enum{{Name: "status", Values: []string{"active", "archived"}}}
	specs, _, err := planGeneratedMigrationSpecs(c.Context(), must.Must(builtin.New()),
		diff, desired, roundingRead(), postgresInfo(capability.Postgres16()), 100, "lower_scale", policy,
		atlasmigrate.Qualifier{},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 2)
	c.Assert(specs[0].Name, qt.Equals, "lower_scale_enum_values")
	c.Assert(specs[1].Name, qt.Equals, "lower_scale_transactional")
	return specs
}

// TestPlanGeneratedMigrationSpecs_SplitPlanChecksTheTransactionalFile writes the check
// into the file that converts the column, the transactional one, and none into the
// enum file that leads it.
func TestPlanGeneratedMigrationSpecs_SplitPlanChecksTheTransactionalFile(t *testing.T) {
	c := qt.New(t)

	specs := planSplitRoundingSpecs(c, DiffPolicy{})

	c.Assert(specs[0].UpSQL, qt.Contains, `ALTER TYPE "status" ADD VALUE 'archived';`)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "+ptah check")
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "fewer digits after the point")
	c.Assert(specs[1].UpSQL, qt.Contains, roundingCheckLine+"\n")
	c.Assert(specs[1].UpSQL, qt.Contains, `ALTER TABLE "prices" ALTER COLUMN "amount" TYPE`)
}

// TestPlanGeneratedMigrationSpecs_SplitPlanWritesTheNoteUnderTheOptIn holds the split
// plan to the opt-in: with the rounding allowed, the transactional file carries the
// note in place of the check, and the enum file carries neither.
func TestPlanGeneratedMigrationSpecs_SplitPlanWritesTheNoteUnderTheOptIn(t *testing.T) {
	c := qt.New(t)

	specs := planSplitRoundingSpecs(c, DiffPolicy{AllowValueRounding: true})

	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "fewer digits after the point")
	c.Assert(specs[1].UpSQL, qt.Contains, "the rounding was allowed when this file was generated, so no check reads "+
		"the rows and the server rounds or cuts each value to the new digits\n")
	c.Assert(specs[1].UpSQL, qt.Not(qt.Contains), "+ptah check")
}

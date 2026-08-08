package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// crossSchemaDomainColumn is the shape from stokaro/ptah#1138: the database
// column is declared with public.status and the desired schema declares its own
// status in another schema and types the column with it. Two domains, two CHECK
// constraints, one name.
//
// Built as PostgreSQL 17.10 reports it: information_schema records a domain
// column under its BASE type with domain_name/domain_schema naming the domain,
// and the reader blanks a domain's own schema for the schema it is reading, so
// the current side's domain carries no schema of its own.
func crossSchemaDomainColumn() (*goschema.Database, *types.DBSchema) {
	database := &types.DBSchema{
		Domains: []types.DBDomain{{Name: "status", BaseType: "text", Check: "VALUE IN ('open','closed')"}},
		Tables: []types.DBTable{{
			Name: "t",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: true},
				{
					Name: "s", DataType: "text", UDTName: "text",
					FormattedType: "status", DomainName: "status", DomainSchema: "public",
					IsNullable: "NO",
				},
			},
		}},
	}
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "s", Type: "other.status"},
		},
		Domains: []goschema.Domain{{
			StructName: "Status", Name: "status", Schema: "other",
			BaseType: "TEXT", Check: "VALUE IN ('open','closed')",
		}},
	}
	return desired, database
}

// TestGenerateSchemaDiffSQL_DomainColumnIsConvertedBeforeTheOldDomainIsDropped
// is the executable half of the identity rule: it is not enough for the
// comparator to see the change, the plan must convert the column BEFORE the
// domain it uses is dropped.
//
// The plan drops a domain the desired schema no longer declares, and PostgreSQL
// drops it with CASCADE, which takes every column still declared with it.
// Measured on PostgreSQL 17.10 against a table holding one row, with the
// comparator seeing only the domain's NAME:
//
//	ptah-compat schema diff --from <public.status> --to <other.status>
//	  DROP DOMAIN IF EXISTS "status" CASCADE;        <- and no ALTER
//	ptah-compat schema apply --url <copy> --to <other.status> --auto-approve
//	  rc=0, "Schema apply completed successfully"
//	  table t = id only; the column and its row were gone
//
// The merge-base, same command and an identical copy of the same database,
// planned ALTER TABLE "t" ALTER COLUMN "s" TYPE other.status ahead of the drop
// and left the row intact. Ordering alone is not the fix and the ALTER alone is
// not either; this asserts both, so a change that restores one without the
// other is red (stokaro/ptah#1138).
func TestGenerateSchemaDiffSQL_DomainColumnIsConvertedBeforeTheOldDomainIsDropped(t *testing.T) {
	c := qt.New(t)

	desired, database := crossSchemaDomainColumn()

	diff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)

	c.Assert(diff.DomainsRemoved, qt.DeepEquals, []string{"status"},
		qt.Commentf("the desired schema no longer declares public.status, so the plan drops it"))

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, desired, platform.Postgres)
	c.Assert(err, qt.IsNil)

	sql := strings.Join(statements, "\n")
	conversion := strings.Index(sql, `ALTER COLUMN "s" TYPE other.status`)
	removal := strings.Index(sql, `DROP DOMAIN IF EXISTS "status" CASCADE`)

	c.Assert(conversion >= 0, qt.IsTrue,
		qt.Commentf("the column must be converted off public.status, or the CASCADE below takes it\n%s", sql))
	c.Assert(removal >= 0, qt.IsTrue,
		qt.Commentf("the plan is expected to drop the domain the desired schema dropped\n%s", sql))
	c.Assert(conversion < removal, qt.IsTrue,
		qt.Commentf("the conversion must come first, or the CASCADE takes the column with it\n%s", sql))
}

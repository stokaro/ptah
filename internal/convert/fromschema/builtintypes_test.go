package fromschema_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// shadowingDocument is the IR a PostgreSQL read produces for a schema whose
// user type answers to a name pg_catalog already answers to, next to a column
// of the built-in type of that name.
//
// Seeded and read on PostgreSQL 17.10:
//
//	CREATE SCHEMA advm; CREATE DOMAIN advm.money AS numeric(12,2);
//	CREATE TABLE advm.prices (id integer, domain_col advm.money, builtin_col money);
//
// inspect wrote `column "builtin_col" { type = money }` and
// `column "domain_col" { type = sql("advm.money") }`, so the read had already
// said which column is which. `declare` is what varies per row: it puts the
// shadowing declaration into the document, and the column type is the bare name
// both of them answer to.
func shadowingDocument(columnType string, declare func(*goschema.Database)) *goschema.Database {
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t", Schema: "advm"}},
		Fields: []goschema.Field{{StructName: "T", Name: "c", Type: columnType}},
	}
	declare(database)
	return database
}

func declareShadowingDomain(database *goschema.Database) {
	database.Domains = []goschema.Domain{{
		Name: "money", Schema: "advm", BaseType: "numeric(12,2)",
	}}
}

func declareShadowingComposite(database *goschema.Database) {
	database.CompositeTypes = []goschema.CompositeType{{
		Name:   "point",
		Schema: "advp",
		Fields: []goschema.CompositeTypeField{{Name: "x", Type: "numeric"}},
	}}
}

func declareShadowingRange(database *goschema.Database) {
	database.Ranges = []goschema.Range{{
		Name: "numrange", Schema: "advr", Subtype: "numeric",
	}}
}

func declareShadowingEnum(database *goschema.Database) {
	database.Enums = []goschema.Enum{{
		Name: "money", Schema: "adve", Values: []string{"lo", "hi"},
	}}
}

// TestQualifyDeclaredUserTypesLeavesABuiltInTypeAlone pins the direction the
// name-keyed rewrite gets wrong: a column whose type is a BUILT-IN keeps it,
// even when a declaration in the same document answers to the same bare name.
//
// Every row is a column that was catalog-correct before stokaro/ptah#1138 and
// silently retyped by it. Measured on PostgreSQL 17.10 for the first row, both
// plans replayed into fresh databases at exit 0 and the catalog read with
// format_type and pg_type.typtype:
//
//	source            builtin_col | money      | pg_catalog | b
//	without the guard builtin_col | advm.money | advm       | d
//	with it           builtin_col | money      | pg_catalog | b
//
// The kinds are enumerated rather than sampled because the map that does the
// rewriting is built from Domains, CompositeTypes, Ranges and Enums in one
// loop each, and a guard added to one loop looks exactly like a guard added to
// all four until a row says otherwise. Both spellings are enumerated for the
// same reason: the scalar and the array spelling read two different maps.
func TestQualifyDeclaredUserTypesLeavesABuiltInTypeAlone(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		declare    func(*goschema.Database)
	}{
		{
			name:       "a domain shadowing money, scalar",
			columnType: "money",
			declare:    declareShadowingDomain,
		},
		{
			name:       "a domain shadowing money, array",
			columnType: "money[]",
			declare:    declareShadowingDomain,
		},
		{
			name:       "a composite shadowing point, scalar",
			columnType: "point",
			declare:    declareShadowingComposite,
		},
		{
			name:       "a composite shadowing point, array",
			columnType: "point[]",
			declare:    declareShadowingComposite,
		},
		{
			name:       "a range shadowing numrange, scalar",
			columnType: "numrange",
			declare:    declareShadowingRange,
		},
		{
			name:       "a range shadowing numrange, array",
			columnType: "numrange[]",
			declare:    declareShadowingRange,
		},
		{
			// The scalar spelling of an enum belongs to handleEnumTypes and
			// stays with stokaro/ptah#1276; see
			// TestFromDatabaseKeepsTheScalarEnumHalfWithIssue1276. This is the
			// spelling stokaro/ptah#1138 added.
			name:       "an enum shadowing money, array",
			columnType: "money[]",
			declare:    declareShadowingEnum,
		},
		{
			// PostgreSQL type names are case insensitive, so a lookup that
			// compares them verbatim protects `money` and retypes `MONEY`.
			name:       "a built-in name the column spells in upper case",
			columnType: "MONEY",
			declare: func(database *goschema.Database) {
				database.Domains = []goschema.Domain{{
					Name: "MONEY", Schema: "advm", BaseType: "numeric(12,2)",
				}}
			},
		},
		{
			// A SERIAL column reaches this pass as the shorthand, not as int4,
			// and `serial` is not a pg_catalog type name -- it is grammar.
			name:       "a grammar spelling pg_catalog does not store",
			columnType: "serial",
			declare: func(database *goschema.Database) {
				database.Domains = []goschema.Domain{{
					Name: "serial", Schema: "advm", BaseType: "integer",
				}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			qualified := fromschema.QualifyDeclaredUserTypes(
				shadowingDocument(test.columnType, test.declare), platform.Postgres,
			)

			c.Assert(qualified.Fields, qt.HasLen, 1)
			c.Assert(qualified.Fields[0].Type, qt.Equals, test.columnType)
		})
	}
}

// TestQualifyDeclaredUserTypesStillNamesASchemaForANameOfItsOwn is the control
// for the row set above, and it is what keeps "leave a built-in alone" from
// becoming "leave everything alone".
//
// Each row is the same shape as a shadowing row -- one declaration, one column
// naming it bare -- with the one difference that the name is not one pg_catalog
// answers to. These are the cells stokaro/ptah#1138 closed, and they have to
// stay closed.
func TestQualifyDeclaredUserTypesStillNamesASchemaForANameOfItsOwn(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		declare    func(*goschema.Database)
		want       string
	}{
		{
			name:       "a domain, scalar",
			columnType: "positive_int",
			declare: func(database *goschema.Database) {
				database.Domains = []goschema.Domain{{
					Name: "positive_int", Schema: "advm", BaseType: "integer",
				}}
			},
			want: "advm.positive_int",
		},
		{
			name:       "a domain, array",
			columnType: "positive_int[]",
			declare: func(database *goschema.Database) {
				database.Domains = []goschema.Domain{{
					Name: "positive_int", Schema: "advm", BaseType: "integer",
				}}
			},
			want: "advm.positive_int[]",
		},
		{
			name:       "a composite, array",
			columnType: "addr[]",
			declare: func(database *goschema.Database) {
				database.CompositeTypes = []goschema.CompositeType{{
					Name:   "addr",
					Schema: "advp",
					Fields: []goschema.CompositeTypeField{{Name: "street", Type: "text"}},
				}}
			},
			want: "advp.addr[]",
		},
		{
			name:       "a range, array",
			columnType: "numrng[]",
			declare: func(database *goschema.Database) {
				database.Ranges = []goschema.Range{{
					Name: "numrng", Schema: "advr", Subtype: "numeric",
				}}
			},
			want: "advr.numrng[]",
		},
		{
			name:       "an enum, array",
			columnType: "mood[]",
			declare: func(database *goschema.Database) {
				database.Enums = []goschema.Enum{{
					Name: "mood", Schema: "adve", Values: []string{"lo", "hi"},
				}}
			},
			want: "adve.mood[]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			qualified := fromschema.QualifyDeclaredUserTypes(
				shadowingDocument(test.columnType, test.declare), platform.Postgres,
			)

			c.Assert(qualified.Fields, qt.HasLen, 1)
			c.Assert(qualified.Fields[0].Type, qt.Equals, test.want)
		})
	}
}

// TestFromDatabaseKeepsTheScalarEnumHalfWithIssue1276 records the boundary of
// this repair, so the split is a pinned decision rather than something the next
// reader has to rediscover from behavior.
//
// handleEnumTypes re-points a bare enum name, and it does so for a BUILT-IN
// column whose name a declared enum shadows. That predates this change --
// stokaro/ptah#1276 added the line, stokaro/ptah#1138 did not touch it -- and it
// is left alone for two measured reasons. On the read path there is nothing left
// to decide by the time the name arrives: inspecting
// `CREATE TYPE adve.money AS ENUM ('lo','hi')` beside an ordinary `money` column
// on PostgreSQL 17.10 writes `type = enum.money` for BOTH columns, because the
// renderer resolves a column type against the declared enum blocks. On the
// annotation path a guard would be a regression, because `type="money"` beside a
// declared enum `money` is an author naming their own type.
//
// The array spelling is the half that IS fixed, and the difference is that a
// catalog keeps the two apart: the same read wrote `sql("money[]")` for the
// built-in array and `sql("adve.money[]")` for the enum array.
func TestFromDatabaseKeepsTheScalarEnumHalfWithIssue1276(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       string
	}{
		{
			// stokaro/ptah#1276's, and still open: the column is a built-in.
			name:       "a scalar enum name, which handleEnumTypes still re-points",
			columnType: "money",
			want:       "\"c\" adve.money",
		},
		{
			// stokaro/ptah#1138's, and closed: a built-in array keeps its type.
			name:       "the array spelling of the same name",
			columnType: "money[]",
			want:       "\"c\" money[]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			database := shadowingDocument(test.columnType, declareShadowingEnum)

			statements := fromschema.FromDatabase(*database, platform.Postgres)

			c.Assert(statements, qt.IsNotNil)

			sql, err := renderer.RenderSQL(platform.Postgres, statements.Statements...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestQualifyDeclaredUserTypesGuardsEveryPostgresFamilySpelling is the dialect
// axis, and it exists because an earlier revision of this file had no row on it.
//
// The guard was keyed on the literal platform.Postgres while
// [fromschema.QualifyDeclaredUserTypes] runs for every dialect, so on the other
// three PostgreSQL-family targets the PASS was present and only the GUARD was
// absent. Measured with the shipped CLI on the commit before the fix, one
// document declaring `CREATE DOMAIN advm.money` beside a `money` column and a
// `money[]` column:
//
//	ptah schema render --dialect postgres     ->  "c" money       "arr" money[]
//	ptah schema render --dialect cockroachdb  ->  "c" advm.money  "arr" advm.money[]
//	ptah schema render --dialect yugabytedb   ->  "c" advm.money  "arr" advm.money[]
//	ptah schema render --dialect spanner      ->  "c" advm.money  "arr" advm.money[]
//
// and the yugabytedb plan replayed into a live YugabyteDB 2026.1.0.0-b118 at
// exit 0, where the catalog then read `c | advm.money | advm | d` against a
// source that read `c | money | pg_catalog | b`. A base type became a domain on
// a second engine, silently, exactly as on PostgreSQL.
//
// The spellings are read out of platform.NormalizeDialect's own switch rather
// than listed here, so this asserts over every spelling ptah accepts -- the
// aliases `pgx`, `crdb`, `ysql` and `google_spanner` included -- and a family
// member added to platform.IsPostgresFamily later is covered without anyone
// editing this file. That is the same reason the guard selects on that
// predicate instead of naming four dialects.
func TestQualifyDeclaredUserTypesGuardsEveryPostgresFamilySpelling(t *testing.T) {
	c := qt.New(t)

	family := slices.DeleteFunc(acceptedSpellings(c.TB), func(spelling string) bool {
		return !platform.IsPostgresFamily(spelling)
	})

	// Extraction control: an empty or truncated family list would make the
	// sweep below pass while comparing nothing.
	for _, canonical := range []string{
		platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		c.Assert(family, qt.Contains, canonical)
	}

	retyped := slices.DeleteFunc(slices.Clone(family), func(spelling string) bool {
		qualified := fromschema.QualifyDeclaredUserTypes(
			shadowingDocument("money[]", declareShadowingDomain), spelling,
		)
		return qualified.Fields[0].Type == "money[]"
	})

	c.Assert(retyped, qt.HasLen, 0, qt.Commentf("these PostgreSQL-family spellings retyped a built-in column"))
}

// TestQualifyDeclaredUserTypesLeavesNonPostgresTargetsToTheirOwnCatalog is the
// non-interference control for the sweep above: the guard reaches the
// PostgreSQL family and stops there.
//
// It is not an approval of a gap. On these two engines the declaration is the
// only thing in reach that answers to `money`, so qualifying the column is the
// right answer rather than an unguarded one, and both halves of that were
// measured rather than assumed:
//
//	MySQL 9.7.1        SELECT CAST(1 AS money)  -> ERROR 1064 (42000), exit 1
//	                   SELECT CAST(1 AS decimal(12,2)) -> 1.00, exit 0
//	ClickHouse 24.8.14 SELECT name FROM system.data_type_families
//	                     WHERE lower(name) IN ('money','decimal','uuid')
//	                     -> Decimal, UUID
//
// The control terms are in both queries on purpose: an empty answer to a broken
// query looks exactly like an absent type.
//
// This is also the row that separates "the PostgreSQL family shares one
// vocabulary" from "one vocabulary for every dialect". Handing these two
// targets pg_catalog's names would strip a qualifier from a user type that is
// the column's only possible meaning.
func TestQualifyDeclaredUserTypesLeavesNonPostgresTargetsToTheirOwnCatalog(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{
			name:    "mysql, which refuses money as a type name",
			dialect: platform.MySQL,
		},
		{
			name:    "clickhouse, whose data_type_families has no money",
			dialect: platform.ClickHouse,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			qualified := fromschema.QualifyDeclaredUserTypes(
				shadowingDocument("money[]", declareShadowingDomain), test.dialect,
			)

			c.Assert(qualified.Fields, qt.HasLen, 1)
			c.Assert(qualified.Fields[0].Type, qt.Equals, "advm.money[]")
		})
	}
}

package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestComparison_AgreesWithTheSwitchesItReplaced pins each built-in
// descriptor's comparison facts to what the dialect switches in
// migration/schemadiff, internal/indexbacking, internal/foreignkeyscope,
// internal/dbexprprobe and internal/tableidentity answered, and the
// two TestComparison_ServerDatabasesAndRespelledIndexes_AgreeWithTheSwitchesTheyReplaced
// pins for every spelling.
func TestComparison_AgreesWithTheSwitchesItReplaced(t *testing.T) {
	mysqlFamily := descriptor.Comparison{
		TimePrecision:                  descriptor.TimePrecision{Compared: true},
		BareTypes:                      descriptor.BareTypes{Stated: true, DecimalPrecision: 10, FloatBits: 24, RealBits: 53, LobTiers: true},
		ColumnPrimaryKeyIsTheTablesKey: true,
		ForeignKeysOutside:             true,
		ForeignKeyBackingIndex:         true,
		DoubleQuotedStrings:            true,
		RestrictIsNoAction:             true,
		PrimaryKeyName:                 "PRIMARY",
		ConstraintComments:             true,
		RoutineNamesFold:               true,
		RoutineTypeAliases:             "mysqlroutine",
		ViewPredicatesAsIntegers:       true,
		ViewSQLSecurity:                true,
		AutoIncrementFlag:              true,
		DefaultGeneratedKind:           "VIRTUAL",
		ServerDatabases:                true,
	}
	mariaDB := mysqlFamily
	mariaDB.ColumnCheckDropByModify = true
	postgresFloats := descriptor.BareTypes{Stated: true, FloatBits: 53, RealBits: 24}
	want := map[string]descriptor.Comparison{
		"postgres": {
			DeclaredSchemas:                true,
			TimePrecision:                  descriptor.TimePrecision{Compared: true, Default: 6},
			BareTypes:                      postgresFloats,
			SerialOwnsSequence:             true,
			ForeignKeysOutside:             true,
			ForeignKeyMatchesKeyInAnyOrder: true,
			ForeignKeyHoldsItsKey:          true,
			ColumnPrimaryKeyIsTheTablesKey: true,
			PrimaryKeyRenames:              true,
			IdentityColumns:                true,
			ExpressionProbes:               "pg-catalog",
			RolePasswords:                  true,
			RoutineTypeAliases:             "pg-aliases",
			QualifiedTypeNames:             true,
			DefaultGeneratedKind:           "STORED",
			ProbesSQLStandardBodies:        true,
		},
		"cockroachdb": {
			DeclaredSchemas:                true,
			TimePrecision:                  descriptor.TimePrecision{Compared: true, Default: 6},
			BareTypes:                      postgresFloats,
			PrimaryKeyRenames:              true,
			IdentityColumns:                true,
			SerialDefault:                  "unique_rowid()",
			ForeignKeysOutside:             true,
			ForeignKeyMatchesKeyInAnyOrder: true,
			ColumnPrimaryKeyIsTheTablesKey: true,
			ExpressionProbes:               "pg-catalog",
			ProbeViewsInSchema:             true,
			RewritesRoutineBodies:          true,
			RolePasswords:                  true,
			RoutineTypeAliases:             "pg-aliases",
			QualifiedTypeNames:             true,
			CatalogTypeNames:               true,
			PrimaryKeyStoresColumns:        true,
		},
		"yugabytedb": {
			DeclaredSchemas:    true,
			TimePrecision:      descriptor.TimePrecision{Compared: true, Default: 6},
			BareTypes:          postgresFloats,
			ExpressionProbes:   "pg-catalog",
			RolePasswords:      true,
			RoutineTypeAliases: "pg-aliases",
			QualifiedTypeNames: true,
		},
		"mysql":   mysqlFamily,
		"mariadb": mariaDB,
		"sqlite": {
			VerbatimTableNames:   true,
			InternalIndexPrefix:  "sqlite_autoindex_",
			AutoIncrementFlag:    true,
			PrimaryKeyNullable:   true,
			RowidTables:          true,
			TypeAffinity:         true,
			DefaultGeneratedKind: "VIRTUAL",
			NoTableComments:      true,
		},
		"sqlserver": {
			TimePrecision:                  descriptor.TimePrecision{Compared: true, Default: 7},
			TemporalDefaultFunctions:       true,
			BareTypes:                      descriptor.BareTypes{Stated: true, DecimalPrecision: 18, VariableLength: 1, FloatBits: 53, RealBits: 24},
			DeclaredSchemas:                true,
			MapsPortableTypes:              true,
			BitStoresBoolean:               true,
			ColumnPrimaryKeyIsTheTablesKey: true,
			UniqueIndexStandsAlone:         true,
			ExpressionProbes:               "sys-catalog",
			AutoIncrementFlag:              true,
			DefaultGeneratedKind:           "PERSISTED",
			RespelledIndexReplaced:         true,
		},
		"clickhouse": {NullableTypes: true},
		"oracle": {
			TimePrecision:                descriptor.TimePrecision{Compared: true, Default: 6},
			BareTypes:                    descriptor.BareTypes{Stated: true, FloatBits: 126, FloatKeepsPrecision: true, IntervalLeading: 2},
			RewritesGeneratedExpressions: true,
			RoutineNamesFold:             true,
			ArgumentModeAfterName:        true,
			MapsDeclaredTypes:            true,
		},
	}
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			c.Assert(dialect.Comparison, qt.Equals, expected)
		})
	}
}

// TestComparison_ServerDatabasesAndRespelledIndexes_AgreeWithTheSwitchesTheyReplaced
// pins the two facts migration/schemadiff read from a dialect switch, for
// every spelling the registry accepts: a whole-server comparison compared the
// databases of MySQL and MariaDB (isMySQLFamilyComparison), and SQL Server
// rebuilt an index whose name differed from the declaration only in spelling
// (indexReplacementRequired).
func TestComparison_ServerDatabasesAndRespelledIndexes_AgreeWithTheSwitchesTheyReplaced(t *testing.T) {
	serverDatabases := map[string]bool{platform.MySQL: true, platform.MariaDB: true}
	respelledIndexReplaced := map[string]bool{platform.SQLServer: true}
	spellings := platform.DialectSpellings()
	c := qt.New(t)
	c.Assert(len(spellings) > len(builtindialects.Descriptors()), qt.IsTrue,
		qt.Commentf("the spellings include the aliases, not only the canonical names"))
	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			canonical := platform.NormalizeDialect(spelling)
			described, found := platform.Lookup(spelling)
			c.Assert(found, qt.IsTrue)
			c.Assert(described.Comparison.ServerDatabases, qt.Equals, serverDatabases[canonical])
			c.Assert(described.Comparison.RespelledIndexReplaced, qt.Equals, respelledIndexReplaced[canonical])
		})
	}
}

package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestComparison_AgreesWithTheSwitchesItReplaced pins each built-in
// descriptor's comparison facts to what the dialect switches in
// migration/schemadiff, internal/indexbacking, internal/foreignkeyscope,
// internal/dbexprprobe and migration/internal/tableidentity answered.
func TestComparison_AgreesWithTheSwitchesItReplaced(t *testing.T) {
	mysqlFamily := descriptor.Comparison{
		ForeignKeysOutside:       true,
		ForeignKeyBackingIndex:   true,
		DoubleQuotedStrings:      true,
		RestrictIsNoAction:       true,
		PrimaryKeyName:           "PRIMARY",
		ConstraintComments:       true,
		RoutineNamesFold:         true,
		RoutineTypeAliases:       "mysqlroutine",
		ViewPredicatesAsIntegers: true,
		AutoIncrementFlag:        true,
		DefaultGeneratedKind:     "VIRTUAL",
	}
	want := map[string]descriptor.Comparison{
		"postgres": {
			ForeignKeysOutside:   true,
			ExpressionProbes:     "pg-catalog",
			RolePasswords:        true,
			RoutineTypeAliases:   "pg-aliases",
			QualifiedTypeNames:   true,
			DefaultGeneratedKind: "STORED",
		},
		"cockroachdb": {
			ForeignKeysOutside:      true,
			ExpressionProbes:        "pg-catalog",
			ProbeViewsInSchema:      true,
			RewritesRoutineBodies:   true,
			RolePasswords:           true,
			RoutineTypeAliases:      "pg-aliases",
			QualifiedTypeNames:      true,
			CatalogTypeNames:        true,
			PrimaryKeyStoresColumns: true,
		},
		"yugabytedb": {
			ExpressionProbes:   "pg-catalog",
			RolePasswords:      true,
			RoutineTypeAliases: "pg-aliases",
			QualifiedTypeNames: true,
		},
		"spanner": {
			ForeignKeyBackingIndex: true,
			RolePasswords:          true,
			RoutineTypeAliases:     "pg-aliases",
			SingleStringType:       true,
		},
		"mysql":   mysqlFamily,
		"mariadb": mysqlFamily,
		"sqlite": {
			VerbatimTableNames:   true,
			InternalIndexPrefix:  "sqlite_autoindex_",
			AutoIncrementFlag:    true,
			PrimaryKeyNullable:   true,
			RowidTables:          true,
			TypeAffinity:         true,
			DefaultGeneratedKind: "VIRTUAL",
		},
		"sqlserver": {
			UniqueIndexStandsAlone: true,
			ExpressionProbes:       "sys-catalog",
			AutoIncrementFlag:      true,
			DefaultGeneratedKind:   "PERSISTED",
		},
		"clickhouse": {NullableTypes: true},
		"oracle": {
			RewritesGeneratedExpressions: true,
			RoutineNamesFold:             true,
			ArgumentModeAfterName:        true,
			MapsDeclaredTypes:            true,
		},
		"ydb": {UniqueIndexStandsAlone: true},
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

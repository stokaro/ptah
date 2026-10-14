package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// loweringFacts are the per-engine facts internal/modelast, internal/deporder
// and migration/generator read from a descriptor.
type loweringFacts struct {
	GeneratedTimeouts        descriptor.GeneratedTimeouts
	GeneratedColumnDefault   string
	InlineEnum               descriptor.InlineEnum
	ForeignKeysInCreateTable bool
	IndexesBeforeForeignKeys bool
	QuotingPaths             bool
	Lexical                  descriptor.Lexical
}

// oldSwitches is what the dialect switches these fields replace answered for
// each engine: the generated timeouts of postgres, mysql and mariadb, the
// generated-column and inline-enum switches of internal/modelast, its SQLite,
// MySQL-family and YDB table rules (YDB's read from Quoting.Paths), and the ClickHouse and YDB scanning rules of
// internal/deporder.
var oldSwitches = map[string]loweringFacts{
	platform.Postgres:    {GeneratedTimeouts: descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"}, GeneratedColumnDefault: "STORED"},
	platform.CockroachDB: {GeneratedColumnDefault: "STORED"},
	platform.YugabyteDB:  {GeneratedColumnDefault: "STORED"},
	platform.Spanner: {GeneratedColumnDefault: "STORED",
		InlineEnum: descriptor.InlineEnum{Type: "TEXT", Check: descriptor.InlineEnumCheckBare}},
	platform.MySQL: {GeneratedTimeouts: descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"}, GeneratedColumnDefault: "VIRTUAL",
		InlineEnum: descriptor.InlineEnum{Type: descriptor.InlineEnumValues}, IndexesBeforeForeignKeys: true},
	platform.MariaDB: {GeneratedTimeouts: descriptor.GeneratedTimeouts{Lock: "3s", Statement: "30s"}, GeneratedColumnDefault: "VIRTUAL",
		InlineEnum: descriptor.InlineEnum{Type: descriptor.InlineEnumValues}, IndexesBeforeForeignKeys: true},
	platform.SQLServer: {GeneratedColumnDefault: "PERSISTED",
		InlineEnum: descriptor.InlineEnum{Type: "NVARCHAR(255)", Check: descriptor.InlineEnumCheckQuoted}},
	platform.Oracle: {InlineEnum: descriptor.InlineEnum{Type: "VARCHAR2(255)", Check: descriptor.InlineEnumCheckQuoted}},
	platform.ClickHouse: {InlineEnum: descriptor.InlineEnum{Type: descriptor.InlineEnumNumbered},
		Lexical: descriptor.Lexical{LineComments: []string{"#", "//"}, BackslashEscapes: true}},
	platform.SQLite: {GeneratedColumnDefault: "VIRTUAL",
		InlineEnum: descriptor.InlineEnum{Type: "TEXT", Check: descriptor.InlineEnumCheckBare}, ForeignKeysInCreateTable: true},
	platform.YDB: {QuotingPaths: true, Lexical: descriptor.Lexical{Lexer: descriptor.LexerYQL}},
}

// TestDescriptors_LoweringFactsAgreeWithTheOldSwitches pins every built-in
// descriptor's lowering facts to what the dialect switch it replaces answered,
// and covers every built-in dialect.
func TestDescriptors_LoweringFactsAgreeWithTheOldSwitches(t *testing.T) {
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(oldSwitches))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			got := loweringFacts{
				GeneratedTimeouts:        dialect.GeneratedTimeouts,
				GeneratedColumnDefault:   dialect.GeneratedColumnDefault,
				InlineEnum:               dialect.InlineEnum,
				ForeignKeysInCreateTable: dialect.ForeignKeysInCreateTable,
				IndexesBeforeForeignKeys: dialect.IndexesBeforeForeignKeys,
				QuotingPaths:             dialect.Quoting.Paths,
				Lexical:                  dialect.Lexical,
			}
			c.Assert(got, qt.CmpEquals(cmpopts.EquateEmpty()), oldSwitches[dialect.Name])
		})
	}
}

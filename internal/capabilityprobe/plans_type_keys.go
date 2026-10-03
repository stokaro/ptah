package capabilityprobe

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withTypeKeys adds the questions the YDB type map and renderer decide with:
// which column types and defaults a target takes, whether SERIAL fills a
// column, and whether an index can be maintained asynchronously.
//
// Every engine answers them, so the registry asks one question everywhere.
// Most answers outside YDB are a refusal, and that refusal is the measurement:
// the day an engine takes `Timestamp64` or `GLOBAL ASYNC`, the row turns red.
// A key that names a column type is judged by acceptance, which is creation
// for a table, except on SQLite, which stores any declared type name and
// keeps only its affinity; there acceptance shows nothing and the key is
// declared undecided. On YDB, which has the types, the CREATE is followed by a
// use of what it made.
func withTypeKeys(p plan, dialect string) plan {
	spelling, ok := typeKeySpellingFor(dialect)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, spelling.experiments()...)
	for key, reason := range spelling.undecided {
		if p.undecided == nil {
			p.undecided = make(map[capability.Capability]string)
		}
		p.undecided[key] = reason
	}
	return p
}

// typeKeySpelling is one dialect's spelling of the type experiments.
type typeKeySpelling struct {
	// table spells a throwaway table with a key column id and the columns
	// given after it.
	table func(name, columns string) string
	// integer and smallInteger are the dialect's spellings of a 32-bit and a
	// 16-bit integer column type.
	integer, smallInteger string
	// decimal is the dialect's spelling of DECIMAL(10,2).
	decimal string
	// serial spells table tk_ser with a SERIAL key id and an integer n.
	serial string
	// uses are checks that use what a key's CREATE TABLE claims to have
	// made, on a dialect that has the types: there a type name the server
	// renamed or a clause it dropped would otherwise read as support. A key
	// with none is decided by the CREATE alone, which is right where the
	// statement is refused and where a day it is accepted should turn the
	// row red.
	uses map[capability.Capability][]check
	// undecided are the keys this dialect declares instead of asking.
	undecided map[capability.Capability]string
}

// sqliteTypeAffinity is why SQLite declares the column-type keys undecided.
const sqliteTypeAffinity = "SQLite stores any declared type name and keeps only its affinity, so a table " +
	"that declares the type is accepted whether or not SQLite has it, and acceptance answers nothing"

func typeKeySpellingFor(dialect string) (typeKeySpelling, bool) {
	keyed := func(idType string) func(name, columns string) string {
		return func(name, columns string) string {
			return fmt.Sprintf("CREATE TABLE %s (id %s NOT NULL, %s, PRIMARY KEY (id))", name, idType, columns)
		}
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.ClickHouse:
		return typeKeySpelling{
			table: func(name, columns string) string {
				return fmt.Sprintf("CREATE TABLE %s (id Int32, %s) ENGINE=MergeTree ORDER BY id", name, columns)
			},
			integer: "Int32", smallInteger: "Int16", decimal: "Decimal(10,2)",
			serial: "CREATE TABLE tk_ser (id SERIAL, n Int32) ENGINE=MergeTree ORDER BY id",
		}, true
	case platform.Oracle:
		return typeKeySpelling{
			table: keyed("NUMBER(10)"), integer: "NUMBER(10)", smallInteger: "SMALLINT", decimal: "DECIMAL(10,2)",
			serial: serialTable("NUMBER(10)"),
		}, true
	case platform.YDB:
		return ydbTypeKeySpelling(), true
	case platform.SQLite:
		return typeKeySpelling{
			table: keyed("INTEGER"), integer: "INTEGER", smallInteger: "SMALLINT", decimal: "DECIMAL(10,2)",
			serial: serialTable("INTEGER"),
			undecided: map[capability.Capability]string{
				capability.WideDateTimeTypes:    sqliteTypeAffinity,
				capability.ParameterizedDecimal: sqliteTypeAffinity,
			},
		}, true
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
		platform.MySQL, platform.MariaDB, platform.SQLServer:
		return typeKeySpelling{
			table: keyed("int"), integer: "int", smallInteger: "smallint", decimal: "DECIMAL(10,2)",
			serial: serialTable("int"),
		}, true
	default:
		return typeKeySpelling{}, false
	}
}

func (t typeKeySpelling) experiments() []experiment {
	created := func(key capability.Capability, statement string) experiment {
		return proven(key, schemaChange{change: []string{statement}, after: t.uses[key]})
	}
	all := []experiment{
		created(capability.WideDateTimeTypes, t.table("tk_wdt", "c Timestamp64")),
		created(capability.ParameterizedDecimal, t.table("tk_pdc", "c "+t.decimal)),
		created(capability.AsyncIndexes, t.table("tk_ain", "n "+t.integer+", INDEX tk_ain_n GLOBAL ASYNC ON (n)")),
		created(capability.DocumentTypeDefaults, t.table("tk_dtd", "c JsonDocument DEFAULT JsonDocument('{}')")),
		proven(capability.SerialColumns, schemaChange{
			change: []string{t.serial},
			after: []check{
				accepts("INSERT INTO tk_ser (n) VALUES (1)"),
				accepts("INSERT INTO tk_ser (n) VALUES (2)"),
				counts("SELECT COUNT(DISTINCT id) FROM tk_ser", 2),
			},
		}),
		proven(capability.SmallIntegerDefaults, schemaChange{
			change: []string{t.table("tk_sid", "c "+t.smallInteger+" DEFAULT 5")},
			after: []check{
				accepts("INSERT INTO tk_sid (id) VALUES (1)"),
				counts("SELECT COUNT(*) FROM tk_sid WHERE c = 5", 1),
			},
		}),
	}
	out := all[:0]
	for _, experiment := range all {
		if _, declared := t.undecided[experiment.decides[0]]; !declared {
			out = append(out, experiment)
		}
	}
	return out
}

// ydbTypeKeySpelling is YDB's spelling, the one dialect that has every type
// the keys name. Each CREATE is followed by a use only the type admits: a
// Timestamp64 before 1970, which Timestamp cannot hold; a decimal compared at
// the precision declared; a default a row receives; and the asynchronous
// index read back through Ptah's reader, since no SQL describes an index.
func ydbTypeKeySpelling() typeKeySpelling {
	return typeKeySpelling{
		table: func(name, columns string) string {
			return fmt.Sprintf("CREATE TABLE %s (id Int32 NOT NULL, %s, PRIMARY KEY (id))", name, columns)
		},
		integer: "Int32", smallInteger: "Int16", decimal: "Decimal(10,2)",
		serial: "CREATE TABLE tk_ser (id Serial, n Int32, PRIMARY KEY (id))",
		uses: map[capability.Capability][]check{
			capability.WideDateTimeTypes: {
				accepts(`INSERT INTO tk_wdt (id, c) VALUES (1, Timestamp64("1900-01-01T00:00:00Z"))`),
				counts(`SELECT COUNT(*) FROM tk_wdt WHERE c < Timestamp64("1970-01-01T00:00:00Z")`, 1),
			},
			capability.ParameterizedDecimal: {
				accepts(`INSERT INTO tk_pdc (id, c) VALUES (1, Decimal("12345678.91", 10, 2))`),
				counts(`SELECT COUNT(*) FROM tk_pdc WHERE c = Decimal("12345678.91", 10, 2)`, 1),
			},
			capability.AsyncIndexes: {
				ydbDescribedIndex("tk_ain", "tk_ain_n", "the index to be GLOBAL ASYNC",
					func(index catalog.Index) bool { return index.Method == "GLOBAL ASYNC" }),
			},
			capability.DocumentTypeDefaults: {
				accepts("INSERT INTO tk_dtd (id) VALUES (1)"),
				counts("SELECT COUNT(*) FROM tk_dtd WHERE id = 1 AND c IS NOT NULL", 1),
			},
		},
	}
}

// serialTable spells table tk_ser with a SERIAL primary key.
func serialTable(integer string) string {
	return fmt.Sprintf("CREATE TABLE tk_ser (id SERIAL, n %s, PRIMARY KEY (id))", integer)
}

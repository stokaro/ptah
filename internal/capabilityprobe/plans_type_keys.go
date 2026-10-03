package capabilityprobe

import (
	"fmt"

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
// declared undecided.
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
	all := []experiment{
		acceptance(capability.WideDateTimeTypes, nil, t.table("tk_wdt", "c Timestamp64")),
		acceptance(capability.ParameterizedDecimal, nil, t.table("tk_pdc", "c "+t.decimal)),
		acceptance(capability.AsyncIndexes, nil,
			t.table("tk_ain", "n "+t.integer+", INDEX tk_ain_n GLOBAL ASYNC ON (n)")),
		acceptance(capability.DocumentTypeDefaults, nil,
			t.table("tk_dtd", "c JsonDocument DEFAULT JsonDocument('{}')")),
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

// serialTable spells table tk_ser with a SERIAL primary key.
func serialTable(integer string) string {
	return fmt.Sprintf("CREATE TABLE tk_ser (id SERIAL, n %s, PRIMARY KEY (id))", integer)
}

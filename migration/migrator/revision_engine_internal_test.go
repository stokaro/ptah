package migrator

// White-box testing required: createMigrationsTableSQL is package-local, and the
// property under test is what the statement says rather than what a server does
// with it.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCreateMigrationsTableSQL_CarriesTheEngineClause pins the native revision
// DDL, the second of the two revision-table formats.
//
// Which engine a dialect gets is decided by revisionEngineClauseFor and covered
// there; what this pins is that the statement carries the clause at all. Without
// it ClickHouse gives the table no engine, and whether an unnamed one is even
// legal is the server's `default_table_engine` -- whose own default value is
// `None`. Measured on ClickHouse 26.7.5.10 with that setting, `migrations up`
// stopped before the first migration:
//
//	code: 119, message: Table engine is not specified in CREATE query
//
// On a server whose default is MergeTree nothing looked wrong and the engine
// came from the server rather than from Ptah, so a replicated deployment got a
// local MergeTree holding a history the other replicas do not have
// (stokaro/ptah#2234).
func TestCreateMigrationsTableSQL_CarriesTheEngineClause(t *testing.T) {
	c := qt.New(t)

	const replicated = "ReplicatedMergeTree('/clickhouse/tables/{shard}/schema_migrations', '{replica}')"
	m := (&Migrator{}).WithMigrationsEngine(replicated)

	c.Assert(m.createMigrationsTableSQL(), qt.Contains, ") ENGINE = "+replicated)
}

// TestCreateMigrationsTableSQL_NamesNoEngineWhereThereIsNone is the control.
//
// A dialect with no engine clause -- which is every one but ClickHouse and the
// MySQL family -- must not gain one: PostgreSQL would refuse the statement
// rather than read it as explicit.
func TestCreateMigrationsTableSQL_NamesNoEngineWhereThereIsNone(t *testing.T) {
	c := qt.New(t)

	c.Assert((&Migrator{}).createMigrationsTableSQL(), qt.Not(qt.Contains), "ENGINE")
}

// TestMigrationsTableCreateError_NamesTheEngineAndWhereItCameFrom pins the
// message.
//
// A ClickHouse engine the revision table cannot use is refused as
// `code: 36, message: Engine Log doesn't support ... ORDER_BY ...`, which says
// what is wrong and not where the engine came from. On a deployment that set it
// through PTAH_MIGRATIONS_ENGINE rather than on the command line, that is the
// difference between a one-line fix and a search.
func TestMigrationsTableCreateError_NamesTheEngineAndWhereItCameFrom(t *testing.T) {
	c := qt.New(t)

	m := (&Migrator{}).WithMigrationsEngine("Log")

	err := m.migrationsTableCreateError(errServerRefused)

	c.Assert(err, qt.ErrorMatches, `.*ENGINE = Log.*--migrations-engine.*PTAH_MIGRATIONS_ENGINE.*`)
	c.Assert(err, qt.ErrorIs, errServerRefused)
}

// TestMigrationsTableCreateError_SaysNothingExtraWithoutAnEngine is that
// message's control: on a dialect with no engine clause there is nothing to
// name, and inventing a suggestion would point at a flag that changes nothing.
func TestMigrationsTableCreateError_SaysNothingExtraWithoutAnEngine(t *testing.T) {
	c := qt.New(t)

	err := (&Migrator{}).migrationsTableCreateError(errServerRefused)

	c.Assert(err, qt.ErrorMatches, `failed to create migrations table: .*`)
	c.Assert(err, qt.ErrorIs, errServerRefused)
}

// errServerRefused stands in for the server refusing the statement.
var errServerRefused = errors.New("code: 36, message: Engine Log does not support ORDER_BY")

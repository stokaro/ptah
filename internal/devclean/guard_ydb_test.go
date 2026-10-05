package devclean_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

func ydbGuard(realm devclean.ReplayRealm) *devclean.ReplayGuard {
	return devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.YDB}, realm)
}

// ydbConfinedStatements stay inside a dev realm: each writes a relative path
// that does not climb out, reads, or defines a value. Ptah renders every one
// of these shapes for a YDB schema, a coordination node's in a statement of
// its own that the connection resolves against the realm's prefix.
var ydbConfinedStatements = []struct {
	name      string
	statement string
}{
	{name: "CREATE TABLE", statement: "CREATE TABLE `users` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"},
	{name: "CREATE TABLE in a schema", statement: "CREATE TABLE IF NOT EXISTS `app/users` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"},
	{name: "CREATE TABLE named with a dot segment", statement: "CREATE TABLE `./app/t` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"},
	{name: "a bare name", statement: "CREATE TABLE users (id Int64 NOT NULL, PRIMARY KEY (id))"},
	{name: "ALTER TABLE ADD COLUMN", statement: "ALTER TABLE `app/users` ADD COLUMN `email` Utf8"},
	{name: "ALTER TABLE ADD INDEX", statement: "ALTER TABLE `users` ADD INDEX `by_email` GLOBAL SYNC ON (`email`)"},
	{name: "ALTER TABLE RENAME INDEX", statement: "ALTER TABLE `users` RENAME INDEX `a` TO `b`"},
	{name: "ALTER TABLE RENAME TO", statement: "ALTER TABLE `users` RENAME TO `app/people`"},
	{name: "DROP TABLE", statement: "DROP TABLE IF EXISTS `app/users`"},
	{name: "CREATE VIEW reading outside", statement: "CREATE VIEW `v` WITH (security_invoker = TRUE) AS SELECT * FROM `/local/users`"},
	{name: "DROP VIEW", statement: "DROP VIEW `v`"},
	{name: "CREATE TOPIC", statement: "CREATE TOPIC IF NOT EXISTS `app/events` (CONSUMER `reader`) WITH (min_active_partitions = 2)"},
	{name: "ALTER TOPIC", statement: "ALTER TOPIC `app/events` ADD CONSUMER `audit`, SET (retention_period = Interval('P1D'))"},
	{name: "DROP TOPIC", statement: "DROP TOPIC IF EXISTS `app/events`"},
	{name: "INSERT", statement: "INSERT INTO `users` (`id`) VALUES (1)"},
	{name: "INSERT OR IGNORE", statement: "INSERT OR IGNORE INTO `users` (`id`) VALUES (1)"},
	{name: "UPSERT from a read outside", statement: "UPSERT INTO `users` SELECT * FROM `/local/users`"},
	{name: "REPLACE", statement: "REPLACE INTO `users` (`id`) VALUES (1)"},
	{name: "UPDATE", statement: "UPDATE `users` SET `email` = 'x'u WHERE `id` = 1"},
	{name: "UPDATE ON", statement: "UPDATE `users` ON SELECT 1 AS `id`, 'x'u AS `email`"},
	{name: "DELETE", statement: "DELETE FROM `users` WHERE `id` = 1"},
	{name: "BATCH UPDATE", statement: "BATCH UPDATE `users` SET `email` = NULL"},
	{name: "a named expression", statement: "$rows = SELECT * FROM `/local/users`"},
	{name: "SELECT", statement: "SELECT * FROM `users`"},
	{name: "DECLARE", statement: "DECLARE $id AS Int64"},
	{name: "DEFINE SUBQUERY", statement: "DEFINE SUBQUERY $q() AS SELECT 1; END DEFINE"},
	{name: "a pragma that moves no name", statement: "PRAGMA AnsiInForEmptyOrNullableItemsCollections"},
	{name: "CREATE COORDINATION NODE", statement: "CREATE COORDINATION NODE `app/locks` WITH (self_check_period = Interval('PT2S'))"},
	{name: "ALTER COORDINATION NODE", statement: "ALTER COORDINATION NODE `locks` SET (read_consistency_mode = 'strict')"},
	{name: "DROP COORDINATION NODE", statement: "DROP COORDINATION NODE `app/locks`"},
}

// Every confined statement passes in a dev realm and on a server the run owns.
func TestReplayGuard_YDBConfinedStatements_HappyPath(t *testing.T) {
	for _, realm := range []devclean.ReplayRealm{devclean.ReplayRealmDatabase, devclean.ReplayRealmServer} {
		guard := ydbGuard(realm)
		for _, test := range ydbConfinedStatements {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
			})
		}
	}
}

// ydbRealmEscapes leave a dev realm, or change what the whole database
// shares, and are lifted on a server the run owns, where the realm is the
// server.
var ydbRealmEscapes = []struct {
	name      string
	statement string
	operation string
}{
	{name: "an absolute path", statement: "CREATE TABLE `/local/users` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		operation: "the absolute path /local/users"},
	{name: "a path that climbs out", statement: "CREATE TABLE `../users` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
		operation: "the path ../users, which climbs out of the dev database"},
	{name: "a climb in the middle", statement: "DROP TABLE `app/../../users`",
		operation: "the path app/../../users, which climbs out of the dev database"},
	{name: "a rename out", statement: "ALTER TABLE `users` RENAME TO `../people`",
		operation: "the path ../people, which climbs out of the dev database"},
	{name: "a rename to an absolute path", statement: "ALTER TABLE `users` RENAME TO `/local/people`",
		operation: "the absolute path /local/people"},
	{name: "an insert through a $ name", statement: "INSERT INTO $p (`id`) VALUES (1)", operation: "a target named through $p"},
	{name: "an absolute insert", statement: "UPSERT INTO `/local/users` (`id`) VALUES (1)", operation: "the absolute path /local/users"},
	{name: "an absolute update", statement: "UPDATE `/local/users` SET `email` = NULL", operation: "the absolute path /local/users"},
	{name: "an absolute delete", statement: "DELETE FROM `/local/users`", operation: "the absolute path /local/users"},
	{name: "a cluster", statement: "INSERT INTO db.`users` (`id`) VALUES (1)", operation: "a target in the cluster db"},
	{name: "a sequence by its absolute path", statement: "ALTER SEQUENCE `/local/users/_serial_column_id` RESTART WITH 5",
		operation: "the absolute path /local/users/_serial_column_id"},
	{name: "the prefix", statement: `PRAGMA TablePathPrefix = "/local"`, operation: "PRAGMA TablePathPrefix"},
	{name: "the prefix in its namespace", statement: `PRAGMA ydb.TablePathPrefix("/local")`, operation: "PRAGMA TablePathPrefix"},
	{name: "USE", statement: "USE db", operation: "USE"},
	{name: "an action", statement: "DEFINE ACTION $a($t) AS INSERT INTO $t (`id`) VALUES (1); END DEFINE", operation: "DEFINE ACTION"},
	{name: "DO", statement: "DO $a(\"/local/users\")", operation: "DO"},
	{name: "EVALUATE", statement: "EVALUATE FOR $t IN AsList(\"/local/users\") DO $a($t)", operation: "EVALUATE"},
	{name: "a user", statement: `CREATE USER u1 PASSWORD "p"`, operation: "a user of the whole database"},
	{name: "a group", statement: "ALTER GROUP readers ADD USER u1", operation: "a group of the whole database"},
	{name: "a grant", statement: "GRANT SELECT ON `/local/users` TO u1", operation: "GRANT permission change"},
	{name: "a revoke", statement: "REVOKE SELECT ON `/local/users` FROM u1", operation: "REVOKE permission change"},
	{name: "a secret", statement: `CREATE OBJECT s (TYPE SECRET) WITH value = "x"`, operation: "a secret or an object of the whole database"},
	{name: "a resource pool", statement: "CREATE RESOURCE POOL p WITH (CONCURRENT_QUERY_LIMIT = 1)", operation: "a resource pool of the whole database"},
	{name: "a backup collection", statement: "CREATE BACKUP COLLECTION c (TABLE `users`) WITH (STORAGE = 'cluster')",
		operation: "a backup collection of the whole database"},
	{name: "a backup", statement: "BACKUP c", operation: "BACKUP"},
	{name: "a topic by its absolute path", statement: "CREATE TOPIC `/local/events`", operation: "the absolute path /local/events"},
	{name: "a topic that climbs out", statement: "DROP TOPIC IF EXISTS `../events`",
		operation: "the path ../events, which climbs out of the dev database"},
	{name: "a statement the guard does not know", statement: "IMPORT m SYMBOLS $f", operation: "unrecognized statement IMPORT"},
	{name: "a coordination node by its absolute path", statement: "DROP COORDINATION NODE `/local/locks`",
		operation: "the absolute path /local/locks"},
	{name: "a coordination node that climbs out", statement: "CREATE COORDINATION NODE `../locks`",
		operation: "the path ../locks, which climbs out of the dev database"},
	{name: "a coordination node named through a $ name", statement: "ALTER COORDINATION NODE $n SET (read_consistency_mode = 'strict')",
		operation: "a target named through $n"},
	{name: "an object the guard does not know", statement: "CREATE TABLESTORE `s`", operation: "unrecognized object CREATE TABLESTORE"},
}

// Each escape is refused in a dev realm, by name.
func TestReplayGuard_YDBRealmEscapes_FailurePath(t *testing.T) {
	guard := ydbGuard(devclean.ReplayRealmDatabase)
	for _, test := range ydbRealmEscapes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches,
				"ydb migration replay rejects "+regexp.QuoteMeta(test.operation)+
					" because its effects cannot be confined to the disposable database realm")
		})
	}
}

// The control for the test above: on a server the run owns the same
// statements pass, so the realm is what refused them.
func TestReplayGuard_YDBRealmEscapes_HappyPathOnAnOwnedServer(t *testing.T) {
	guard := ydbGuard(devclean.ReplayRealmServer)
	for _, test := range ydbRealmEscapes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// What reaches past the server is refused on a server the run owns too, and a
// translation setting, whose effect on the rest of the text the guard cannot
// read.
func TestReplayGuard_YDBBeyondTheServer_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{name: "an external data source", statement: "CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = \"ObjectStorage\")",
			operation: "an external data source"},
		{name: "a TTL tier", statement: "ALTER TABLE `t` SET (TTL = Interval(\"P1D\") TO EXTERNAL DATA SOURCE `/local/s` ON `ts`)",
			operation: "an external data source"},
		{name: "an external table", statement: "DROP EXTERNAL TABLE `e`", operation: "an external table"},
		{name: "async replication", statement: "CREATE ASYNC REPLICATION r FOR `a` AS `b` WITH (CONNECTION_STRING = \"grpc://h/db\")",
			operation: "async replication"},
		{name: "a transfer", statement: "CREATE TRANSFER t FROM `topic` TO `table` USING $f", operation: "a transfer"},
		{name: "a streaming query", statement: "CREATE OR REPLACE STREAMING QUERY q AS DO BEGIN SELECT 1; END DO",
			operation: "a streaming query"},
		{name: "a translation setting", statement: "--!syntax_v0\nSELECT 1", operation: "translation setting --!syntax_v0"},
	}
	for _, realm := range []devclean.ReplayRealm{devclean.ReplayRealmDatabase, devclean.ReplayRealmServer} {
		guard := ydbGuard(realm)
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches,
					"ydb migration replay rejects "+regexp.QuoteMeta(test.operation)+
						" because its effects cannot be confined to the disposable database realm")
			})
		}
	}
}

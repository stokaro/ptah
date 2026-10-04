package capabilityprobe

import (
	"context"
	"fmt"
	"path"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
)

// ydbSpelling writes the throwaway tables the YDB experiments take. A YDB table
// needs a key (`Primary key is required for ydb tables.`) declared in a clause
// of its own: `id Int64 PRIMARY KEY` is a parse error on every line.
var ydbSpelling = tableSpelling{keyed: true}

// ydbPlan is the statement table for YDB.
//
// Where YDB has a spelling of its own for what a key names, the experiment
// asks it in that spelling and then uses what the statement claims to have
// made, because acceptance is not creation on this engine: an INDEX ...
// GLOBAL inline in a column table's CREATE TABLE is accepted and creates no
// index. Where YDB has no spelling, the experiment sends the standard SQL one,
// so the day a release takes it the row turns red; a CREATE in such an
// experiment is followed by a use, so taking the word and dropping it does
// not read as support.
//
// Statements are written unqualified. The session sends each after the pragma
// that makes the run's directory the place an unqualified name means, and the
// directory leaves with everything in it at the end.
//
// No experiment opens a transaction block: YQL has no BEGIN, a transaction is
// the driver's wrapper around a query, and YDB refuses a schema statement
// inside one. The CONCURRENTLY keys are therefore decided by acceptance, as on
// ClickHouse, and the keys about a migration's transaction are declared.
//
// Every statement here was measured by hand on local-ydb 25.1.4.7, 25.2.1.24,
// 25.3.1.25, 25.4.1.15, 26.1.1.22 and 26.2.1.14.
func ydbPlan() plan {
	t := ydbSpelling
	experiments := []experiment{
		// The grammar has no CONSTRAINT clause, so the table that would carry
		// the named constraint is part of the question rather than its setup:
		// a refused setup decides nothing, and this refusal is the answer.
		all(capability.DropConstraintGeneric, nil,
			t.table("dcg", "n Int64 NOT NULL, CONSTRAINT dcg_uq UNIQUE (n)", "n"),
			"ALTER TABLE dcg DROP CONSTRAINT dcg_uq",
		),
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{t.table("dcie", "n Int64 NOT NULL", "n")},
			decide: guarded(capability.DropConstraintIfExists, nil,
				[]string{"ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent"},
				"ALTER TABLE dcie DROP CONSTRAINT dcie_absent",
			).decide,
		},
		// The renderer's own spelling: YDB drops an index through its table.
		guarded(capability.DropIndexIfExists,
			[]string{t.table("dii", "id Int64 NOT NULL, n Int64, INDEX dii_n GLOBAL ON (n)", "id")},
			[]string{"ALTER TABLE dii DROP INDEX IF EXISTS dii_absent"},
			"ALTER TABLE dii DROP INDEX dii_absent",
		),
		guarded(capability.ObjectExistenceGuards, nil,
			[]string{"DROP TABLE IF EXISTS oeg_absent", "DROP VIEW IF EXISTS oeg_absent_view"},
			"DROP TABLE oeg_absent",
		),
		proven(capability.CheckConstraintsEnforced, schemaChange{
			change: []string{t.table("cce", "n Int64 NOT NULL, CONSTRAINT cce_ck CHECK (n > 0)", "n")},
			after: []check{
				accepts("INSERT INTO cce (n) VALUES (1)"),
				refuses("INSERT INTO cce (n) VALUES (-1)"),
			},
		}),
		all(capability.DropCheckClause, nil,
			t.table("dcc", "n Int64 NOT NULL, CONSTRAINT dcc_ck CHECK (n > 0)", "n"),
			"ALTER TABLE dcc DROP CHECK dcc_ck",
		),
		// YQL has an Enum type, and a table cannot hold one; this is the
		// spelling a YDB enum column would have.
		acceptanceNote(capability.EnumInlineColumn, nil,
			t.table("eic", "id Int64 NOT NULL, c Enum<'a', 'b'>", "id"),
			"YQL's own Enum type, refused as a column type with `Only YQL data types and PG types are "+
				"currently supported`",
		),
		acceptance(capability.EnumCustomType, nil, "CREATE TYPE ect AS ENUM ('a', 'b')"),
		acceptanceNote(capability.CreateIndexConcurrently,
			[]string{t.table("cic", "id Int64 NOT NULL, n Int64", "id")},
			"CREATE INDEX CONCURRENTLY cic_one ON cic (n)",
			"acceptance decides it here: YDB has no CREATE INDEX at all and answers `no viable "+
				"alternative at input 'CREATE INDEX'`, which is not what a parsed no-op does",
		),
		acceptance(capability.DropIndexConcurrently,
			[]string{t.table("dic", "id Int64 NOT NULL, n Int64, INDEX dic_n GLOBAL ON (n)", "id")},
			"ALTER TABLE dic DROP INDEX CONCURRENTLY dic_n",
		),
		uninspectableIndexIncludeSPGiST(
			[]string{t.table("iis", "k Utf8 NOT NULL, payload Int64", "k")},
			"CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload)",
		),
		// The option is mandatory (YDB refuses a view without it), and the
		// count proves the view reads its source.
		proven(capability.Views, schemaChange{
			setup:  []string{t.table("vsrc", "n Int64 NOT NULL", "n"), "INSERT INTO vsrc (n) VALUES (1)"},
			change: []string{"CREATE VIEW vw WITH (security_invoker = TRUE) AS SELECT n FROM vsrc"},
			after:  []check{counts("SELECT COUNT(*) FROM vw", 1)},
		}),
		storedResult(capability.MaterializedViews,
			[]string{t.table("mvs", "n Int64 NOT NULL", "n")},
			"CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs",
			"SELECT c FROM mvw",
			"INSERT INTO mvs (n) VALUES (1)",
		),
		all(capability.Functions, nil, "CREATE FUNCTION fn() RETURNS Int64 LANGUAGE SQL AS 'SELECT 1'", "SELECT fn()"),
		all(capability.Procedures, nil, "CREATE PROCEDURE pr() LANGUAGE SQL AS 'SELECT 1'", "CALL pr()"),
		all(capability.Triggers,
			[]string{t.table("trg_t", "n Int64 NOT NULL", "n")},
			"CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_fn()",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup:    []string{t.table("cort_t", "n Int64 NOT NULL", "n")},
			decide: acceptance(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW EXECUTE FUNCTION cort_fn()",
			).decide,
		},
		proven(capability.GeneratedColumns, schemaChange{
			change: []string{t.table("gcx", "n Int64 NOT NULL, g Int64 GENERATED ALWAYS AS (n + 1) STORED", "n")},
			after: []check{
				accepts("INSERT INTO gcx (n) VALUES (1)"),
				counts("SELECT COUNT(*) FROM gcx WHERE g = 2", 1),
			},
		}),
		{
			decides:  []capability.Capability{capability.AlterGeneratedColumnExpression},
			requires: []capability.Capability{capability.GeneratedColumns},
			setup:    []string{t.table("agc", "n Int64 NOT NULL, g Int64 GENERATED ALWAYS AS (n + 1) STORED", "n")},
			decide: acceptance(capability.AlterGeneratedColumnExpression, nil,
				"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
			).decide,
		},
		all(capability.RowLevelSecurity,
			[]string{t.table("rls", "n Int64 NOT NULL", "n")},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
			"CREATE POLICY rls_p ON rls USING (true)",
		),
		ydbRoleManagement(t),
		foreignKeys(
			[]string{t.table("fk_parent", "id Int64 NOT NULL", "id"), t.table("fk_child", "id Int64 NOT NULL", "id")},
			"ALTER TABLE fk_child ADD CONSTRAINT fk_child_fk FOREIGN KEY (id) REFERENCES fk_parent (id)",
			"INSERT INTO fk_parent (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (99)",
		),
		referencePolicy(referencePolicyStatements{
			setup: []string{
				t.table("fkp_uni", "id Int64 NOT NULL, k Int64 NOT NULL, INDEX fkp_uni_uq GLOBAL UNIQUE SYNC ON (k)", "id"),
				t.table("fkp_idx", "id Int64 NOT NULL, k Int64 NOT NULL, INDEX fkp_idx_k GLOBAL ON (k)", "id"),
				t.table("fkp_none", "id Int64 NOT NULL, k Int64 NOT NULL", "id"),
				t.table("fkp_c0", "id Int64 NOT NULL, k Int64", "id"),
				t.table("fkp_c1", "id Int64 NOT NULL, k Int64", "id"),
				t.table("fkp_c2", "id Int64 NOT NULL, k Int64", "id"),
			},
			unique:  "ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k)",
			indexed: "ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k)",
			bare:    "ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k)",
		}),
		// A Serial column's sequence is the serial_columns key; this one is
		// the standalone object.
		all(capability.Sequences, nil, "CREATE SEQUENCE sq", "SELECT NEXT VALUE FOR sq"),
		{
			decides:  []capability.Capability{capability.SequenceStartCounterOnly},
			requires: []capability.Capability{capability.Sequences},
			decide: enforced(capability.SequenceStartCounterOnly, nil,
				"CREATE SEQUENCE sso_ok",
				"CREATE SEQUENCE sso_no INCREMENT BY 2",
			).decide,
		},
		all(capability.DomainTypes, nil,
			"CREATE DOMAIN utp_dom AS Utf8",
			t.table("utp_dt", "id Int64 NOT NULL, c utp_dom", "id"),
		),
		all(capability.CompositeTypes, nil,
			"CREATE TYPE utp_comp AS (a Int64, b Utf8)",
			t.table("utp_ct", "id Int64 NOT NULL, c utp_comp", "id"),
		),
		all(capability.RangeTypes, nil,
			"CREATE TYPE utp_range AS RANGE (SUBTYPE = Int64)",
			t.table("utp_rt", "id Int64 NOT NULL, c utp_range", "id"),
		),
		schemaComments(),
		acceptance(capability.XMLType, nil, t.table("xmlt", "id Int64 NOT NULL, c XML", "id")),
		all(capability.AdvisoryLocks, nil, "SELECT pg_advisory_lock(1)", "SELECT pg_advisory_unlock(1)"),
		acceptanceNote(capability.CheckGrantStatement, nil,
			"CHECK GRANT SELECT ON *.*",
			"the statement is ClickHouse's; a server without it refuses the syntax",
		),
		proven(capability.UniqueConstraints, schemaChange{
			change: []string{t.table("uqc", "id Int64 NOT NULL, n Int64, CONSTRAINT uqc_uq UNIQUE (n)", "id")},
			after: []check{
				accepts("INSERT INTO uqc (id, n) VALUES (1, 10)"),
				refuses("INSERT INTO uqc (id, n) VALUES (2, 10)"),
			},
		}),
		// The clause on YDB's own unique index, which a CREATE TABLE takes on
		// every line: the setup is that index without the clause, so a refusal
		// here is the clause's.
		acceptance(capability.UniqueNullsDistinctClause,
			[]string{t.table("ndc_ctl", "id Int64 NOT NULL, n Int64, INDEX ndc_ctl_uq GLOBAL UNIQUE SYNC ON (n)", "id")},
			t.table("ndc", "id Int64 NOT NULL, n Int64, INDEX ndc_uq GLOBAL UNIQUE SYNC ON (n) NULLS NOT DISTINCT", "id"),
		),
		acceptance(capability.ForeignKeyDeleteColumnList,
			[]string{t.table("fdp", "id Int64 NOT NULL", "id"), t.table("fdc", "n Int64 NOT NULL, id Int64", "n")},
			"ALTER TABLE fdc ADD CONSTRAINT fdc_fk FOREIGN KEY (id) REFERENCES fdp (id) ON DELETE SET NULL (id)",
		),
		acceptance(capability.DeferrableConstraints,
			[]string{t.table("dfp", "id Int64 NOT NULL", "id"), t.table("dfc", "n Int64 NOT NULL, id Int64", "n")},
			"ALTER TABLE dfc ADD CONSTRAINT dfc_fk FOREIGN KEY (id) REFERENCES dfp (id) DEFERRABLE INITIALLY DEFERRED",
		),
		proven(capability.RenameColumnClause, schemaChange{
			setup:  []string{t.table("rnc_t", "n Int64 NOT NULL, b Int64", "n")},
			change: []string{"ALTER TABLE rnc_t RENAME COLUMN b TO c"},
			after:  []check{accepts("INSERT INTO rnc_t (n, c) VALUES (1, 1)")},
		}),
		ydbRowDeletionPolicy(t),
		acceptanceNote(capability.NamedNotNullConstraints, nil,
			t.table("nnn", "id Int64 CONSTRAINT nnn_named NOT NULL", "id"),
			"YQL has no CONSTRAINT clause, so no NOT NULL carries a name for a catalog to report",
		),
		acceptanceNote(capability.RowLevelTTL, nil,
			t.table("ttlp", "id Int64 NOT NULL, expires_at Timestamp", "id")+
				" WITH (ttl_expiration_expression = 'expires_at')",
			"the key names CockroachDB's storage parameter; YDB answers `Unknown table setting`, and its own "+
				"TTL is the row_deletion_policy row",
		),
		acceptance(capability.AlterTableAlgorithmLock,
			[]string{t.table("aal", "n Int64 NOT NULL", "n")},
			"ALTER TABLE aal ADD COLUMN m Int64, ALGORITHM=INPLACE, LOCK=NONE",
		),
		all(capability.AddConstraintNotValid,
			[]string{t.table("nvc", "n Int64 NOT NULL", "n")},
			"ALTER TABLE nvc ADD CONSTRAINT nvc_ck CHECK (n > 0) NOT VALID",
			"ALTER TABLE nvc VALIDATE CONSTRAINT nvc_ck",
		),
	}
	experiments = append(experiments, ydbCatalogExperiments()...)
	return plan{experiments: experiments, undecided: ydbUndecided()}
}

// ydbCatalogExperiments asks the catalog reads the PostgreSQL and MySQL
// families make. YDB has no information_schema and no pg_catalog: the reader
// asks the scheme and table services instead, so each of these is refused,
// and a release that answered one would turn its row red.
func ydbCatalogExperiments() []experiment {
	const note = "a PostgreSQL-family catalog read; YDB has no pg_catalog or information_schema, and its " +
		"reader asks the scheme and table services instead"
	return []experiment{
		acceptanceNote(capability.PostgresCatalogFunctions, nil, "SELECT obj_description(2200, 'pg_namespace')", note),
		acceptanceNote(capability.CatalogRowStatistics, nil, "SELECT 1 FROM pg_stat_all_tables LIMIT 1", note),
		acceptanceNote(capability.CatalogDependencies, nil, "SELECT 1 FROM pg_depend LIMIT 1", note),
		acceptanceNote(capability.CatalogDefaultPrivileges, nil, "SELECT 1 FROM pg_default_acl LIMIT 1", note),
		acceptanceNote(capability.CatalogTriggerDefinitions, nil,
			"SELECT pg_get_triggerdef(oid) FROM pg_trigger LIMIT 1", note),
		acceptanceNote(capability.CatalogViewDependencies, nil,
			"SELECT 1 FROM information_schema.view_table_usage LIMIT 1", note),
		acceptanceNote(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints LIMIT 1", note),
		acceptanceNote(capability.CatalogPartitions, nil, "SELECT 1 FROM pg_inherits LIMIT 1", note),
		acceptanceNote(capability.CatalogRecursiveCTE, nil,
			"WITH RECURSIVE m AS (SELECT relname FROM pg_class) SELECT relname FROM m LIMIT 1", note),
	}
}

// ydbUndecided names the keys no statement this probe sends can decide on
// YDB, each for a reason that holds of the dialect rather than of one run.
func ydbUndecided() map[capability.Capability]string {
	return map[capability.Capability]string{
		capability.CatalogVectorInfo: "ALL_TAB_COLS.VECTOR_INFO is an Oracle catalog column; this server has no such " +
			"relation, and the reader the key gates runs only against Oracle, so neither having nor lacking it " +
			"here would decide the key",
		capability.DDLInsideTransaction: "the key names whether the server takes a schema statement inside the " +
			"transaction the migrator opens; YQL has no BEGIN for the probe to open one with, and the driver's " +
			"transaction is the migrator's wrapper rather than a statement this probe sends",
		capability.TransactionalDDL: "the key names whether a failed migration rolls back as a unit, which is how " +
			"YDB runs schema statements rather than one statement's answer",
		capability.MigrationLockTimeout: "the key names a runtime policy the migrator applies around a migration, and " +
			"YDB has no lock-wait setting for a statement to send: a schema statement on a table under another schema " +
			"operation fails at once rather than waiting",
		capability.MigrationStatementTimeout: "the key names a runtime policy the migrator applies around a migration: " +
			"a deadline on each query, and the cancellation of a build it stops, which no statement this probe sends " +
			"can show",
		capability.ShowRoutinePrivilege: "the probe cannot ask whether a privilege exists without granting it, and " +
			"YDB has no routines for a SHOW_ROUTINE privilege to cover",
		capability.Hypertables: "create_hypertable is a TimescaleDB function, and TimescaleDB is a PostgreSQL " +
			"extension YDB has no spelling of",
		capability.ContinuousAggregates: "a TimescaleDB continuous aggregate is a PostgreSQL materialized view with " +
			"an extension option, which YDB has no spelling of",
	}
}

// ydbRoleManagement decides RoleManagement in YDB's own access model: a group,
// a GRANT of a YDB permission on a table, and the grant read back from
// .sys/auth_permissions, where the access model lives.
//
// Three measurements shape it. A group name takes no underscore (`Name is not
// allowed`), so the group is named from the namespace without them. GRANT
// ignores the namespace pragma and needs the table's absolute path: a relative
// one answers `Path does not exist` on 26.2 and `wrong path format` on 25.1.
// And a group outlives the namespace, so it is recorded for the teardown.
func ydbRoleManagement(t tableSpelling) experiment {
	return experiment{
		decides: []capability.Capability{capability.RoleManagement},
		setup:   []string{t.table("rm_t", "n Int64 NOT NULL", "n")},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			group := strings.ReplaceAll(s.namespace, "_", "")
			created := s.exec(ctx, "CREATE GROUP "+group)
			attempts := []Attempt{created}
			if !created.Accepted {
				return verdicts{capability.RoleManagement: decided(false)}, attempts
			}
			s.roles = append(s.roles, group)
			table := path.Join(s.database, s.namespace, "rm_t")
			granted := s.exec(ctx, "GRANT SELECT ON "+sqlident.Quote(platform.YDB, table)+" TO "+group)
			attempts = append(attempts, granted)
			if !granted.Accepted {
				return verdicts{capability.RoleManagement: decided(false)}, attempts
			}
			stored, read := s.query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE Sid = %s AND Path = %s",
				ydbSystemView(s.database, "auth_permissions"), ydbString(group), ydbString(table)))
			attempts = append(attempts, read)
			return verdicts{capability.RoleManagement: grantObservation(read, stored)}, attempts
		},
	}
}

// grantObservation reads the read-back after an accepted GRANT: a row is a
// grant the access model holds, and none is one the server took and dropped.
func grantObservation(read Attempt, stored int64) observation {
	if !read.Accepted {
		return cannotDecide("the GRANT was accepted and the read-back %q was refused (%s)",
			collapse(read.Statement), collapse(read.ServerErr))
	}
	if stored < 1 {
		return annotated(false, "the server accepted the GRANT and .sys/auth_permissions does not report it")
	}
	return decided(true)
}

// ydbRowDeletionPolicy decides RowDeletionPolicy with YDB's TTL, the same idea
// as Spanner's clause: one interval and one column. No SQL reads a TTL back on
// every line, so the proof is a use: the column the policy names cannot be
// dropped while the policy stands (`Can't drop TTL column: 'created_at',
// disable TTL first`), a column it does not name can, and once the policy is
// reset the named column drops too.
func ydbRowDeletionPolicy(t tableSpelling) experiment {
	return proven(capability.RowDeletionPolicy, schemaChange{
		change: []string{
			t.table("rdp", "id Int64 NOT NULL, created_at Timestamp, other Int64", "id") +
				` WITH (TTL = Interval("P30D") ON created_at)`,
		},
		after: []check{
			accepts("ALTER TABLE rdp DROP COLUMN other"),
			refuses("ALTER TABLE rdp DROP COLUMN created_at"),
			accepts("ALTER TABLE rdp RESET (TTL)"),
			accepts("ALTER TABLE rdp DROP COLUMN created_at"),
		},
	})
}

// ydbDescribedIndex is a check that reads an index back through Ptah's own YDB
// reader, scoped to the probe's directory, and holds when want answers true
// for what it read. The reader is the question the plan asks, because no SQL
// describes an index on YDB and a read the reader cannot make is one no
// comparison could converge on.
func ydbDescribedIndex(table, index, expectation string, want func(catalog.Index) bool) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read index %s of table %s through Ptah's YDB reader",
				index, path.Join(s.database, s.namespace, table))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Indexes {
				if found.TableName == table && found.Name == index {
					return attempt, want(found), "read " + found.Definition
				}
			}
			return attempt, false, "found no such index"
		},
	}
}

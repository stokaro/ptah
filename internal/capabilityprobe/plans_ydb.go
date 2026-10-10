package capabilityprobe

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbview"
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
		// The option is mandatory (YDB refuses a view without it), the count
		// proves the view reads its source, and Ptah's reader has to describe
		// it with its query, which the server stores without the comment.
		proven(capability.Views, schemaChange{
			setup:  []string{t.table("vsrc", "n Int64 NOT NULL", "n"), "INSERT INTO vsrc (n) VALUES (1)"},
			change: []string{"CREATE VIEW vw " + ydbview.SecurityClause + " AS SELECT n FROM vsrc -- read back"},
			after: []check{
				counts("SELECT COUNT(*) FROM vw", 1),
				ydbDescribedView("vw", "SELECT n FROM vsrc"),
			},
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
		ydbAccessControl(t),
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
		// The swap a planned table rebuild ends with. The new name resolves
		// against the namespace's TablePathPrefix, as every name here does,
		// and the table is read back under it and gone under the old one.
		proven(capability.RenameTable, schemaChange{
			setup:  []string{t.table("rnt", "n Int64 NOT NULL", "n")},
			change: []string{renameTableStatement},
			after:  []check{accepts("SELECT COUNT(*) FROM rnt2"), refuses("SELECT COUNT(*) FROM rnt")},
		}),
		ydbRowDeletionPolicy(t),
		ydbRowDeletionPolicyEpochColumn(t),
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

// ydbAccessControl decides the access keys in YDB's own model: a group and a
// user, the user made a member of the group, and three grants -- on a table of
// the namespace by its absolute path, on the database itself, and on the
// namespace directory by its single relative name -- each read back through
// Ptah's YDB reader, which is the question a plan asks.
//
// Measurements shape it. A user or group name takes lower-case letters and
// digits only (`Name is not allowed`), so the principals are named from the
// namespace without its underscores. GRANT ignores the namespace pragma and
// resolves a relative path against the database root; a single name resolves
// on 26.1 and later and answers `wrong path format` on 25.1 to 25.4. And the
// principals and the database grant outlive the namespace, so they are recorded
// for the teardown, which revokes the grant before it drops them: DROP USER
// and DROP GROUP leave a principal's entries behind.
func ydbAccessControl(t tableSpelling) experiment {
	keys := []capability.Capability{
		capability.RoleManagement, capability.GroupPrincipals, capability.RoleMembership,
		capability.DatabaseGrants, capability.RelativeGrantPaths,
	}
	return experiment{
		decides: keys,
		setup:   []string{t.table("rm_t", "n Int64 NOT NULL", "n")},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			principal := strings.ReplaceAll(s.namespace, "_", "")
			group, user := principal+"g", principal+"u"
			created := s.exec(ctx, "CREATE GROUP "+group)
			attempts := []Attempt{created}
			if !created.Accepted {
				return allDecided(keys, false), attempts
			}
			s.roles = append(s.roles, group)
			createdUser := s.exec(ctx, "CREATE USER "+user+" NOLOGIN")
			attempts = append(attempts, createdUser)
			if createdUser.Accepted {
				s.users = append(s.users, user)
			}
			table := path.Join(s.database, s.namespace, "rm_t")
			statements := []string{
				"ALTER GROUP " + group + " ADD USER " + user,
				"GRANT 'ydb.generic.read' ON " + sqlident.Quote(platform.YDB, table) + " TO " + group,
				"GRANT 'ydb.generic.list' ON " + sqlident.Quote(platform.YDB, s.database) + " TO " + group,
				"GRANT 'ydb.granular.describe_schema' ON " + sqlident.Quote(platform.YDB, s.namespace) + " TO " + group,
			}
			accepted := make([]bool, len(statements))
			for i, statement := range statements {
				attempt := s.exec(ctx, statement)
				attempts = append(attempts, attempt)
				accepted[i] = attempt.Accepted
			}
			read := Attempt{Statement: "read the users, groups, memberships and grants of " +
				path.Join(s.database, s.namespace) + " through Ptah's YDB reader"}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				read.ServerErr = err.Error()
				attempts = append(attempts, read)
				return allUndecided(keys, "the read-back was refused (%s)", collapse(err.Error())), attempts
			}
			read.Accepted = true
			attempts = append(attempts, read)
			held := func(grant catalog.Grant) bool { return slices.Contains(db.Grants, grant) }
			return verdicts{
				capability.RoleManagement: readBack{
					accepted: true, what: "the grant on the table",
					found: held(catalog.Grant{Role: group, Privilege: "ydb.generic.read",
						ObjectType: "TABLE", Schema: s.namespace, ObjectName: "rm_t"}),
				}.observation(),
				capability.GroupPrincipals: readBack{
					accepted: createdUser.Accepted, statement: createdUser.Statement, what: "a group beside a user",
					found: slices.ContainsFunc(db.Roles, func(role catalog.Role) bool { return role.Name == group && role.Inherit && role.Group }) &&
						slices.ContainsFunc(db.Roles, func(role catalog.Role) bool { return role.Name == user && role.Inherit && !role.Group }),
				}.observation(),
				capability.RoleMembership: readBack{
					accepted: accepted[0], statement: statements[0], what: "the member",
					found: slices.Contains(db.RoleMemberships, catalog.RoleMembership{Role: group, Member: user}),
				}.observation(),
				capability.DatabaseGrants: readBack{
					accepted: accepted[2], statement: statements[2], what: "the grant on the database",
					found: held(catalog.Grant{Role: group, Privilege: "ydb.generic.list", ObjectType: "DATABASE"}),
				}.observation(),
				capability.RelativeGrantPaths: readBack{
					accepted: accepted[3], statement: statements[3],
					what: "the grant on the directory named by its single relative name",
					found: held(catalog.Grant{Role: group, Privilege: "ydb.granular.describe_schema",
						ObjectType: "SCHEMA", ObjectName: s.namespace}),
				}.observation(),
			}, attempts
		},
	}
}

// readBack is a key a statement and its read-back settle together.
type readBack struct {
	// accepted is whether the server took the statement, and statement is
	// the statement, which a refusal names.
	accepted  bool
	statement string
	// found is whether Ptah's reader reported what the statement made, and
	// what names it in a note.
	found bool
	what  string
}

// observation is the key's answer: a refused statement is false, and an
// accepted one is what the reader reported.
func (r readBack) observation() observation {
	switch {
	case !r.accepted:
		return annotated(false, fmt.Sprintf("the server refused %q", collapse(r.statement)))
	case !r.found:
		return annotated(false, "Ptah's YDB reader does not report "+r.what)
	default:
		return decided(true)
	}
}

// allDecided answers every key alike.
func allDecided(keys []capability.Capability, does bool) verdicts {
	answered := make(verdicts, len(keys))
	for _, key := range keys {
		answered[key] = decided(does)
	}
	return answered
}

// allUndecided leaves every key undecided for one reason.
func allUndecided(keys []capability.Capability, format string, args ...any) verdicts {
	answered := make(verdicts, len(keys))
	for _, key := range keys {
		answered[key] = cannotDecide(format, args...)
	}
	return answered
}

// ydbRowDeletionPolicy decides RowDeletionPolicy with YDB's TTL, the same idea
// as Spanner's clause: one interval and one column. The proof reads the policy
// back through Ptah's own reader, where DescribeTable reports the column and
// the seconds, and then uses it: the column the policy names cannot be
// dropped while the policy stands (`Can't drop TTL column: 'created_at',
// disable TTL first`), a column it does not name can, and once the policy is
// reset the named column drops too.
func ydbRowDeletionPolicy(t tableSpelling) experiment {
	return proven(capability.RowDeletionPolicy, schemaChange{
		change: []string{
			t.table("rdp", "id Int64 NOT NULL, created_at Timestamp, other Int64", "id") +
				` WITH (TTL = Interval("PT720H") ON created_at)`,
		},
		after: []check{
			ydbDescribedPolicy("rdp", &ydbschema.TTL{Column: "created_at", Interval: "P30D"}),
			accepts("ALTER TABLE rdp DROP COLUMN other"),
			refuses("ALTER TABLE rdp DROP COLUMN created_at"),
			accepts("ALTER TABLE rdp RESET (TTL)"),
			ydbDescribedPolicy("rdp", nil),
			accepts("ALTER TABLE rdp DROP COLUMN created_at"),
		},
	})
}

// ydbRowDeletionPolicyEpochColumn decides RowDeletionPolicyEpochColumn with a
// TTL on a Uint64 column counting seconds, read back through Ptah's reader with
// its unit, and then used: the column cannot be dropped while the TTL reads
// it. It presupposes the policy itself, so a run that did not decide that key
// true does not ask.
func ydbRowDeletionPolicyEpochColumn(t tableSpelling) experiment {
	decided := proven(capability.RowDeletionPolicyEpochColumn, schemaChange{
		change: []string{
			t.table("rdpe", "id Int64 NOT NULL, expires Uint64", "id") +
				` WITH (TTL = Interval("PT1H") ON expires AS SECONDS)`,
		},
		after: []check{
			ydbDescribedPolicy("rdpe", &ydbschema.TTL{Column: "expires", Interval: "PT1H", Unit: "SECONDS"}),
			refuses("ALTER TABLE rdpe DROP COLUMN expires"),
		},
	})
	decided.requires = []capability.Capability{capability.RowDeletionPolicy}
	return decided
}

// ydbDescribedPolicy is a check that reads a table's row deletion policy back
// through Ptah's own YDB reader, scoped to the probe's directory, and holds
// when it is want; nil wants none. The reader is the question, because a
// policy the reader cannot read back is one no comparison could converge on.
func ydbDescribedPolicy(table string, want *ydbschema.TTL) check {
	expectation := "no row deletion policy"
	if want != nil {
		expectation = fmt.Sprintf("the row deletion policy %s %s %s", want.Column, want.Interval, want.Unit)
	}
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read the TTL of table %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, table))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Tables {
				if found.Name != table {
					continue
				}
				observed, _, err := schemaext.FacetAs[*ydbschema.ObservedTTL](found.Facets, ydbschema.TTLKind)
				if err != nil {
					attempt.ServerErr = err.Error()
					return attempt, false, "read a TTL Ptah could not take"
				}
				if observed == nil {
					return attempt, want == nil, "read no row deletion policy"
				}
				read := &observed.Policy
				held := want != nil && *read == *want
				return attempt, held, fmt.Sprintf("read the row deletion policy %s %s %s", read.Column, read.Interval, read.Unit)
			}
			return attempt, false, "found no such table"
		},
	}
}

// ydbDescribedView reads view through Ptah's YDB reader and holds when the
// reader describes it with a query that reads as query does: the server keeps
// its own form of the text, which [ydbview.QueryText] reads both sides into.
// It is what proves a view the experiment created is one Ptah reads back,
// which the server's acceptance of CREATE VIEW does not say.
//
// The session sends its path pragma ahead of every statement, and YDB stores
// the pragmas a CREATE VIEW ran under ahead of the view's query, because they
// decide what the query's names mean. So the query the check expects carries
// the session's pragma too.
func ydbDescribedView(view, query string) check {
	return check{
		describes: fmt.Sprintf("view %s described with the query %q", view, query),
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			query := s.prefix + query
			attempt := Attempt{Statement: fmt.Sprintf("read view %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, view))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Views {
				if found.Name == view {
					return attempt, ydbview.QueryText(found.Body) == ydbview.QueryText(query),
						"read the query " + strconv.Quote(found.Body)
				}
			}
			return attempt, false, "found no such view"
		},
	}
}

// ydbDescribedIndex is a check that reads an index back through Ptah's own YDB
// reader, scoped to the probe's directory, and holds when want answers true
// for what it read. The reader is the question the plan asks, because no SQL
// describes an index on YDB and a read the reader cannot make is one no
// comparison could converge on.
func ydbDescribedIndex(table, index, expectation string, want func(catalog.Index) bool) check {
	inNamespace := func(s *session) string { return s.namespace }
	return readBackIndex(table, index, expectation, inNamespace, want, func(s *session) string {
		return fmt.Sprintf("read index %s of table %s through Ptah's YDB reader",
			index, path.Join(s.database, s.namespace, table))
	})
}

// readBackIndex is a check that reads index of table back through Ptah's
// reader and answers whether want holds of it. schema names the schema the
// reader is scoped to, which is the probe's namespace on YDB and dbo inside the
// probe's database on SQL Server; statement names the read in the probe's
// report.
func readBackIndex(
	table, index, expectation string,
	schema func(*session) string,
	want func(catalog.Index) bool,
	statement func(*session) string,
) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: statement(s)}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{schema(s)})
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

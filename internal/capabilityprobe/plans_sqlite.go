package capabilityprobe

import (
	"context"

	"ptah.run/core/platform/capability"
)

// sqlitePlan is the statement table for SQLite.
//
// It is the one plan with no server behind it. SQLite is a library, so the
// engine measured here is the modernc.org/sqlite amalgamation pinned in
// go.mod, reached through an in-memory database rather than a container.
//
// Three of its rows are why the table reads the way it does.
//
// DropConstraintGeneric is decided by two drops rather than one, because the
// engine takes the clause for one constraint kind and refuses it for the other:
// measured on 3.53.4, `ALTER TABLE t DROP CONSTRAINT ck` removes a CHECK and
// the table stops enforcing it, while the same statement against a UNIQUE
// answers `constraint may not be dropped`. The key stands for both, so the row
// is false and carries the split.
//
// XMLType is decided by a value rather than by the CREATE TABLE, because SQLite
// refuses no type name at all: a column declared XML is accepted, and so is
// `not xml <` as its content. An acceptance experiment would have scored the
// key true on an engine with no XML type.
//
// ForeignKeys is decided by a row, for the same reason. SQLite has no ALTER
// TABLE ADD CONSTRAINT, so the reference is declared in the CREATE TABLE the
// other plans use as setup, and what separates a parsed clause from an enforced
// one is the INSERT that violates it (stokaro/ptah#3191).
func sqlitePlan() plan {
	experiments := []experiment{
		sqliteDropConstraintGeneric(),
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{"CREATE TABLE dcie (n INTEGER)"},
			decide: guarded(capability.DropConstraintIfExists, nil,
				[]string{"ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent"},
				"ALTER TABLE dcie DROP CONSTRAINT dcie_absent",
			).decide,
		},
		guarded(capability.DropIndexIfExists, nil,
			[]string{"DROP INDEX IF EXISTS dii_absent"},
			"DROP INDEX dii_absent",
		),
		// Both halves of the key, because a target may take one and not the
		// other. SQLite takes both.
		guarded(capability.ObjectExistenceGuards, nil,
			[]string{
				"CREATE TABLE IF NOT EXISTS oeg (n INTEGER)",
				"DROP TABLE IF EXISTS oeg_absent",
			},
			"DROP TABLE oeg_absent",
		),
		enforced(capability.CheckConstraintsEnforced,
			[]string{"CREATE TABLE cce (n INTEGER, CONSTRAINT cce_ck CHECK (n > 0))"},
			"INSERT INTO cce (n) VALUES (1)",
			"INSERT INTO cce (n) VALUES (-1)",
		),
		acceptance(capability.DropCheckClause,
			[]string{"CREATE TABLE dcc (n INTEGER, CONSTRAINT dcc_ck CHECK (n > 0))"},
			"ALTER TABLE dcc DROP CHECK dcc_ck",
		),
		acceptance(capability.EnumInlineColumn, nil,
			"CREATE TABLE eic (c ENUM('a','b'))",
		),
		// The second statement is what separates a type from a parse
		// everywhere else. Here it cannot run at all: SQLite has no CREATE
		// TYPE, so the first statement answers and the sequence stops.
		all(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a','b')",
			"CREATE TABLE ect_t (c ect)",
		),
		all(capability.CompositeTypes, nil,
			"CREATE TYPE cpt AS (a INTEGER, b TEXT)",
			"CREATE TABLE cpt_t (c cpt)",
		),
		all(capability.RangeTypes, nil,
			"CREATE TYPE rgt AS RANGE (SUBTYPE = INTEGER)",
			"CREATE TABLE rgt_t (c rgt)",
		),
		all(capability.DomainTypes, nil,
			"CREATE DOMAIN dmt AS INTEGER NOT NULL",
			"CREATE TABLE dmt_t (c dmt)",
		),
		acceptance(capability.CreateIndexConcurrently,
			[]string{"CREATE TABLE cic (n INTEGER)"},
			"CREATE INDEX CONCURRENTLY cic_ix ON cic (n)",
		),
		acceptance(capability.DropIndexConcurrently,
			[]string{"CREATE TABLE dic (n INTEGER)", "CREATE INDEX dic_ix ON dic (n)"},
			"DROP INDEX CONCURRENTLY dic_ix",
		),
		acceptance(capability.IndexIncludeSPGiST,
			[]string{"CREATE TABLE spg (n INTEGER, m INTEGER)"},
			"CREATE INDEX spg_ix ON spg USING SPGIST (n) INCLUDE (m)",
		),
		acceptance(capability.Views,
			[]string{"CREATE TABLE vsrc (n INTEGER)"},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		acceptance(capability.MaterializedViews,
			[]string{"CREATE TABLE mvsrc (n INTEGER)"},
			"CREATE MATERIALIZED VIEW mvw AS SELECT n FROM mvsrc",
		),
		acceptance(capability.Functions, nil,
			"CREATE FUNCTION fnc () RETURNS INTEGER AS 'SELECT 1'",
		),
		acceptance(capability.Procedures, nil,
			"CREATE PROCEDURE prc AS SELECT 1",
		),
		acceptance(capability.Triggers,
			[]string{"CREATE TABLE trg_t (n INTEGER)"},
			"CREATE TRIGGER trg AFTER INSERT ON trg_t BEGIN SELECT 1; END",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup:    []string{"CREATE TABLE cort_t (n INTEGER)"},
			decide: all(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort AFTER INSERT ON cort_t BEGIN SELECT 1; END",
				"CREATE OR REPLACE TRIGGER cort AFTER INSERT ON cort_t BEGIN SELECT 1; END",
			).decide,
		},
		acceptance(capability.GeneratedColumns, nil,
			"CREATE TABLE gcl (n INTEGER, g INTEGER GENERATED ALWAYS AS (n * 2) STORED)",
		),
		{
			decides:  []capability.Capability{capability.AlterGeneratedColumnExpression},
			requires: []capability.Capability{capability.GeneratedColumns},
			setup:    []string{"CREATE TABLE agc (n INTEGER, g INTEGER GENERATED ALWAYS AS (n + 1) STORED)"},
			decide: acceptance(capability.AlterGeneratedColumnExpression, nil,
				"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
			).decide,
		},
		acceptance(capability.RowLevelSecurity,
			[]string{"CREATE TABLE rls (n INTEGER)"},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
		),
		// One statement rather than the create-and-grant pair the other plans
		// send: SQLite has no principal to create, so the first statement is
		// the whole answer and a GRANT after it would have nothing to name.
		acceptance(capability.RoleManagement,
			[]string{"CREATE TABLE rm_t (n INTEGER)"},
			"CREATE ROLE ptah_capprobe_role",
		),
		// The row, not the clause. See the package comment.
		enforced(capability.ForeignKeys,
			[]string{
				"CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)",
				"CREATE TABLE fk_child (parent_id INTEGER REFERENCES fk_parent (id))",
				"INSERT INTO fk_parent (id) VALUES (1)",
			},
			"INSERT INTO fk_child (parent_id) VALUES (1)",
			"INSERT INTO fk_child (parent_id) VALUES (2)",
		),
		// The same shape one level down. The three reference policies are a
		// mutual-exclusion group, and on an engine that accepts every
		// REFERENCES clause at declaration time the discriminator is the
		// INSERT: measured, a child of a non-unique parent key answers
		// `foreign key mismatch` on the first row written, NULL included.
		referencePolicy(referencePolicyStatements{
			setup: []string{
				"CREATE TABLE rp_unique (id INTEGER PRIMARY KEY)",
				"CREATE TABLE rp_indexed (v INTEGER)",
				"CREATE INDEX rp_indexed_ix ON rp_indexed (v)",
				"CREATE TABLE rp_bare (v INTEGER)",
				"CREATE TABLE rp_u_child (u INTEGER REFERENCES rp_unique (id))",
				"CREATE TABLE rp_i_child (i INTEGER REFERENCES rp_indexed (v))",
				"CREATE TABLE rp_b_child (b INTEGER REFERENCES rp_bare (v))",
			},
			unique:  "INSERT INTO rp_u_child (u) VALUES (NULL)",
			indexed: "INSERT INTO rp_i_child (i) VALUES (NULL)",
			bare:    "INSERT INTO rp_b_child (b) VALUES (NULL)",
		}),
		acceptance(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
		),
		{
			// A restriction on CREATE SEQUENCE cannot be measured where there
			// is no CREATE SEQUENCE: both spellings would be refused for the
			// missing statement, and the control that makes the answer
			// readable is the one that fails first.
			decides:  []capability.Capability{capability.SequenceStartCounterOnly},
			requires: []capability.Capability{capability.Sequences},
			decide: enforced(capability.SequenceStartCounterOnly, nil,
				"CREATE SEQUENCE sso_ok",
				"CREATE SEQUENCE sso_no INCREMENT BY 2",
			).decide,
		},
		acceptance(capability.SchemaComments, nil,
			"COMMENT ON SCHEMA main IS 'probe'",
		),
		// The value, not the column. See the package comment.
		enforced(capability.XMLType,
			[]string{"CREATE TABLE xmlt (c XML)"},
			"INSERT INTO xmlt (c) VALUES ('<a/>')",
			"INSERT INTO xmlt (c) VALUES ('not xml <')",
		),
		acceptance(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1)",
		),
		acceptance(capability.RowLevelTTL,
			[]string{"CREATE TABLE ttl (n INTEGER)"},
			"ALTER TABLE ttl SET (ttl_expiration_expression = 'n')",
		),
		acceptance(capability.CheckGrantStatement,
			[]string{"CREATE TABLE cgs (n INTEGER)"},
			"CHECK GRANT SELECT ON cgs",
		),
		acceptance(capability.RenameColumnClause,
			[]string{"CREATE TABLE rcc (n INTEGER)"},
			"ALTER TABLE rcc RENAME COLUMN n TO m",
		),
		acceptance(capability.UniqueConstraints, nil,
			"CREATE TABLE uqc (n INTEGER NOT NULL, CONSTRAINT uqc_uq UNIQUE (n))",
		),
		acceptance(capability.UniqueNullsDistinctClause,
			[]string{"CREATE TABLE ndc (n INTEGER)"},
			"CREATE UNIQUE INDEX ndc_uq ON ndc (n) NULLS NOT DISTINCT",
		),
		// Declared in the CREATE TABLE, because SQLite has no ALTER TABLE ADD
		// CONSTRAINT to hang the clause on.
		acceptance(capability.DeferrableConstraints,
			[]string{"CREATE TABLE dfc_parent (id INTEGER PRIMARY KEY)"},
			"CREATE TABLE dfc_child (parent_id INTEGER, CONSTRAINT dfc_c FOREIGN KEY (parent_id) "+
				"REFERENCES dfc_parent (id) DEFERRABLE INITIALLY DEFERRED)",
		),
		// The read-back rather than the acceptance. SQLite takes the name in
		// the CREATE TABLE and keeps it only inside the statement text it
		// stores; the column catalog a reader asks reports no constraint at
		// all, so a name written here is one no comparison could converge on.
		storedNotNullName(nil,
			"CREATE TABLE nnc (n INTEGER CONSTRAINT nnc_nn NOT NULL)",
			"SELECT COUNT(*) FROM pragma_table_info('nnc') WHERE name = 'nnc_nn'",
		),
		acceptance(capability.PostgresCatalogFunctions, nil,
			"SELECT obj_description(1)",
		),
		acceptance(capability.CatalogRowStatistics, nil,
			"SELECT COUNT(*) FROM pg_stat_all_tables",
		),
		acceptance(capability.CatalogDependencies, nil,
			"SELECT COUNT(*) FROM pg_depend",
		),
		acceptance(capability.CatalogDefaultPrivileges, nil,
			"SELECT COUNT(*) FROM pg_default_acl",
		),
		acceptance(capability.CatalogPartitions, nil,
			"SELECT COUNT(*) FROM pg_inherits",
		),
		// SQLite writes a recursive common table expression with the RECURSIVE
		// keyword and has no pg_class to read, so this statement is refused
		// for the catalog rather than for the grammar. The key names both
		// halves and the plan sends the spelling it names.
		acceptance(capability.CatalogRecursiveCTE, nil,
			"WITH RECURSIVE m AS (SELECT relname FROM pg_class) SELECT COUNT(*) FROM m",
		),
		acceptance(capability.CatalogViewDependencies, nil,
			"SELECT COUNT(*) FROM information_schema.view_table_usage",
		),
		acceptance(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints WHERE 1 = 0",
		),
		sqliteDDLInsideTransaction(),
	}

	return plan{experiments: experiments, undecided: map[capability.Capability]string{
		capability.RowDeletionPolicy: "the key names a table clause Ptah renders, reads and plans only " +
			"for Spanner, whose PostgreSQL interface stores it; SQLite has no such clause, so a refusal " +
			"would answer a different question",
		capability.Hypertables: "create_hypertable is a TimescaleDB function, and TimescaleDB is a PostgreSQL " +
			"extension SQLite has no spelling of; its refusal would answer a different question",
		capability.ContinuousAggregates: "a TimescaleDB continuous aggregate is a PostgreSQL materialized " +
			"view with an extension option, which SQLite has no spelling of",
		capability.TransactionalDDL: "the key names whether a failed migration rolls back as a unit, which one " +
			"accepted statement cannot show; it is decided by the engine's DDL semantics rather than by a statement",
		capability.MigrationTimeouts: "the key names a runtime policy the migrator applies around a migration, not a " +
			"statement this probe can send",
		capability.ShowRoutinePrivilege: "the key names a MySQL global privilege. SQLite has no principal to hold " +
			"one and no statement that grants anything, so there is nothing to ask",
	}}
}

// sqliteDropConstraintGeneric decides DropConstraintGeneric by dropping both
// constraint kinds the key stands for.
//
// One statement is not enough here, and this is the only engine in the registry
// where that is true. Measured on 3.53.4: the CHECK drop is accepted and takes
// effect -- the table stops refusing the row that violated it, and
// sqlite_master reports the constraint gone -- while the UNIQUE drop answers
// `constraint may not be dropped`. A plan that asked only the first would have
// promised a planner a clause the engine refuses half the time.
func sqliteDropConstraintGeneric() experiment {
	return experiment{
		decides: []capability.Capability{capability.DropConstraintGeneric},
		setup: []string{
			"CREATE TABLE dcg (n INTEGER, CONSTRAINT dcg_ck CHECK (n > 0))",
			"CREATE TABLE dcg_uq (n INTEGER, CONSTRAINT dcg_uq_uq UNIQUE (n))",
		},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			check := s.exec(ctx, "ALTER TABLE dcg DROP CONSTRAINT dcg_ck")
			unique := s.exec(ctx, "ALTER TABLE dcg_uq DROP CONSTRAINT dcg_uq_uq")
			attempts := []Attempt{check, unique}
			if check.Accepted == unique.Accepted {
				return verdicts{capability.DropConstraintGeneric: decided(check.Accepted)}, attempts
			}
			return verdicts{capability.DropConstraintGeneric: annotated(false,
				"the clause is taken for one constraint kind and refused for the other, and the key stands "+
					"for both: a CHECK drop was "+outcome(check)+" and a UNIQUE drop was "+outcome(unique),
			)}, attempts
		},
	}
}

// outcome renders one attempt's verdict as a word, for a note that has to name
// which half of a pair answered.
func outcome(attempt Attempt) string {
	if attempt.Accepted {
		return "accepted"
	}
	return "refused"
}

// sqliteDDLInsideTransaction decides whether the server takes a schema
// statement inside an explicit transaction block.
//
// The other plans declare this key undecidable, because on those engines the
// answer belongs to the wrapper the migrator opens rather than to a statement.
// SQLite answers it directly: BEGIN opens a block, the schema statement runs
// inside it, and the ROLLBACK leaves the table as the setup made it.
//
// It decides the narrow key only. Whether a failed migration rolls back as a
// unit is capability.TransactionalDDL, which this plan declares undecidable
// beside it.
func sqliteDDLInsideTransaction() experiment {
	return experiment{
		decides: []capability.Capability{capability.DDLInsideTransaction},
		setup:   []string{"CREATE TABLE dit (n INTEGER)"},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempts, started := s.tryInTransaction(ctx, "ALTER TABLE dit ADD COLUMN m INTEGER")
			if !started {
				return verdicts{capability.DDLInsideTransaction: cannotDecide(
					"the server refused %q (%s), so the schema statement never had a transaction block to be inside of",
					collapse(attempts[0].Statement), collapse(attempts[0].ServerErr),
				)}, attempts
			}
			return verdicts{capability.DDLInsideTransaction: decided(attempts[1].Accepted)}, attempts
		},
	}
}

package capabilityprobe

import (
	"context"

	"ptah.run/core/platform/capability"
)

// sqlServerPlan is the statement table for SQL Server.
//
// Two of its rows are the reason the table reads the way it does.
//
// ObjectExistenceGuards is decided by a pair rather than by the DROP guard the
// other plans send, because SQL Server takes one half of the key and not the
// other: `DROP TABLE IF EXISTS` is accepted and `CREATE TABLE IF NOT EXISTS` is
// `Incorrect syntax near the keyword 'IF'`. A plan that asked only about the
// drop would have scored the key true and told the renderer it may write a
// guard T-SQL does not parse.
//
// NamedNotNullConstraints is the trap capability.NamedNotNullConstraints was
// written for, sprung by a second engine: `CREATE TABLE t (n int CONSTRAINT
// nnc_nn NOT NULL)` is ACCEPTED on 17.0.4075.5, and the name reaches no
// catalog -- sys.objects, sys.check_constraints, sys.key_constraints and
// information_schema.table_constraints all report zero rows for it. An
// acceptance experiment would have scored it true; the read-back is what
// decides it.
//
// Every statement here was run against mcr.microsoft.com/mssql/server:2025-latest,
// reporting 17.0.4075.5 (stokaro/ptah#3190).
func sqlServerPlan() plan {
	experiments := []experiment{
		acceptance(capability.DropConstraintGeneric,
			[]string{"CREATE TABLE dcg (n int, CONSTRAINT dcg_ck CHECK (n > 0))"},
			"ALTER TABLE dcg DROP CONSTRAINT dcg_ck",
		),
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{"CREATE TABLE dcie (n int)"},
			decide: guarded(capability.DropConstraintIfExists, nil,
				[]string{"ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent"},
				"ALTER TABLE dcie DROP CONSTRAINT dcie_absent",
			).decide,
		},
		// The index guard needs the table the index would be on, in both
		// spellings: T-SQL carries the target on DROP INDEX rather than
		// resolving the index name on its own.
		guarded(capability.DropIndexIfExists,
			[]string{"CREATE TABLE dii (n int)"},
			[]string{"DROP INDEX IF EXISTS dii_absent ON dii"},
			"DROP INDEX dii_absent ON dii",
		),
		// Both halves, for the reason the package comment gives. The CREATE
		// guard is asked first, so the evidence names the spelling that
		// answered rather than the one that happened to work.
		guarded(capability.ObjectExistenceGuards, nil,
			[]string{
				"CREATE TABLE IF NOT EXISTS oeg (n int)",
				"DROP TABLE IF EXISTS oeg_absent",
			},
			"DROP TABLE oeg_absent",
		),
		enforced(capability.CheckConstraintsEnforced,
			[]string{"CREATE TABLE cce (n int, CONSTRAINT cce_ck CHECK (n > 0))"},
			"INSERT INTO cce (n) VALUES (1)",
			"INSERT INTO cce (n) VALUES (-1)",
		),
		acceptance(capability.DropCheckClause,
			[]string{"CREATE TABLE dcc (n int, CONSTRAINT dcc_ck CHECK (n > 0))"},
			"ALTER TABLE dcc DROP CHECK dcc_ck",
		),
		acceptance(capability.EnumInlineColumn, nil,
			"CREATE TABLE eic (c ENUM('a','b'))",
		),
		// The second statement is what separates a type from a parse. SQL
		// Server has CREATE TYPE for its own alias and table types, so a plan
		// that stopped at acceptance would be reading the statement's first
		// two words.
		all(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a','b')",
			"CREATE TABLE ect_t (c ect)",
		),
		all(capability.CompositeTypes, nil,
			"CREATE TYPE cpt AS (a int, b varchar(10))",
			"CREATE TABLE cpt_t (c cpt)",
		),
		all(capability.RangeTypes, nil,
			"CREATE TYPE rgt AS RANGE (SUBTYPE = int)",
			"CREATE TABLE rgt_t (c rgt)",
		),
		all(capability.DomainTypes, nil,
			"CREATE DOMAIN dmt AS int NOT NULL",
			"CREATE TABLE dmt_t (c dmt)",
		),
		acceptance(capability.CreateIndexConcurrently,
			[]string{"CREATE TABLE cic (n int)"},
			"CREATE INDEX CONCURRENTLY cic_ix ON cic (n)",
		),
		acceptance(capability.DropIndexConcurrently,
			[]string{"CREATE TABLE dic (n int)", "CREATE INDEX dic_ix ON dic (n)"},
			"DROP INDEX CONCURRENTLY dic_ix ON dic",
		),
		// SQL Server has INCLUDE on an index and no access method to name, so
		// the refusal is the USING clause. The key names the SP-GiST pairing,
		// which is what makes that the right statement to send.
		acceptance(capability.IndexIncludeSPGiST,
			[]string{"CREATE TABLE spg (n int, m int)"},
			"CREATE INDEX spg_ix ON spg USING SPGIST (n) INCLUDE (m)",
		),
		acceptance(capability.Views,
			[]string{"CREATE TABLE vsrc (n int)"},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		acceptance(capability.MaterializedViews,
			[]string{"CREATE TABLE mvsrc (n int)"},
			"CREATE MATERIALIZED VIEW mvw AS SELECT n FROM mvsrc",
		),
		acceptance(capability.Functions, nil,
			"CREATE FUNCTION fnc () RETURNS int AS BEGIN RETURN 1 END",
		),
		acceptance(capability.Procedures, nil,
			"CREATE PROCEDURE prc AS SELECT 1",
		),
		acceptance(capability.Triggers,
			[]string{"CREATE TABLE trg_t (n int)"},
			"CREATE TRIGGER trg ON trg_t AFTER INSERT AS SET NOCOUNT ON",
		),
		{
			// Twice, because replacing is the question. A server that only
			// creates answers the first statement and refuses the second.
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup:    []string{"CREATE TABLE cort_t (n int)"},
			decide: all(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR ALTER TRIGGER cort ON cort_t AFTER INSERT AS SET NOCOUNT ON",
				"CREATE OR ALTER TRIGGER cort ON cort_t AFTER INSERT AS SET NOCOUNT ON",
			).decide,
		},
		// The SQL-standard spelling, which is the only one Ptah's renderers
		// emit. SQL Server has computed columns under `AS (<expr>)`, and the
		// key names the clause rather than the ability.
		acceptance(capability.GeneratedColumns, nil,
			"CREATE TABLE gcl (n int, g int GENERATED ALWAYS AS (n * 2) STORED)",
		),
		{
			decides:  []capability.Capability{capability.AlterGeneratedColumnExpression},
			requires: []capability.Capability{capability.GeneratedColumns},
			setup:    []string{"CREATE TABLE agc (n int, g int GENERATED ALWAYS AS (n + 1) STORED)"},
			decide: acceptance(capability.AlterGeneratedColumnExpression, nil,
				"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
			).decide,
		},
		sqlServerRowLevelSecurity(),
		// A SQL Server role is database-scoped, so the throwaway namespace
		// takes it away and there is no cleanup to register. That is the whole
		// difference from the Oracle experiment beside this one, where a role
		// outlives the account that created it and a forgotten one made the
		// NEXT run read a name conflict as an engine without roles.
		all(capability.RoleManagement,
			[]string{"CREATE TABLE rm_t (n int)"},
			"CREATE ROLE ptah_capprobe_role",
			"GRANT SELECT ON rm_t TO ptah_capprobe_role",
		),
		acceptance(capability.ForeignKeys,
			[]string{
				"CREATE TABLE fk_parent (id int PRIMARY KEY)",
				"CREATE TABLE fk_child (parent_id int)",
			},
			"ALTER TABLE fk_child ADD CONSTRAINT fk_c FOREIGN KEY (parent_id) REFERENCES fk_parent (id)",
		),
		referencePolicy(referencePolicyStatements{
			setup: []string{
				"CREATE TABLE rp_unique (id int PRIMARY KEY)",
				"CREATE TABLE rp_indexed (v int)",
				"CREATE INDEX rp_indexed_ix ON rp_indexed (v)",
				"CREATE TABLE rp_bare (v int)",
				"CREATE TABLE rp_child (u int, i int, b int)",
			},
			unique:  "ALTER TABLE rp_child ADD CONSTRAINT rp_u FOREIGN KEY (u) REFERENCES rp_unique (id)",
			indexed: "ALTER TABLE rp_child ADD CONSTRAINT rp_i FOREIGN KEY (i) REFERENCES rp_indexed (v)",
			bare:    "ALTER TABLE rp_child ADD CONSTRAINT rp_b FOREIGN KEY (b) REFERENCES rp_bare (v)",
		}),
		acceptance(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
		),
		enforced(capability.SequenceStartCounterOnly, nil,
			"CREATE SEQUENCE sso_ok",
			"CREATE SEQUENCE sso_no INCREMENT BY 2",
		),
		acceptanceNote(capability.SchemaComments, nil,
			"COMMENT ON SCHEMA dbo IS 'probe'",
			"T-SQL has no COMMENT ON statement at all; SQL Server carries the same idea as an extended "+
				"property written through sp_addextendedproperty, which this key does not name",
		),
		acceptance(capability.XMLType, nil,
			"CREATE TABLE xmlt (c XML)",
		),
		acceptance(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1)",
		),
		acceptance(capability.RowLevelTTL,
			[]string{"CREATE TABLE ttl (n int)"},
			"ALTER TABLE ttl SET (ttl_expiration_expression = 'n')",
		),
		acceptance(capability.CheckGrantStatement,
			[]string{"CREATE TABLE cgs (n int)"},
			"CHECK GRANT SELECT ON cgs",
		),
		acceptanceNote(capability.RenameColumnClause,
			[]string{"CREATE TABLE rcc (n int)"},
			"ALTER TABLE rcc RENAME COLUMN n TO m",
			"SQL Server renames a column through the sp_rename procedure, which is a different statement "+
				"from the ALTER clause this key names",
		),
		acceptance(capability.UniqueConstraints, nil,
			"CREATE TABLE uqc (n int NOT NULL, CONSTRAINT uqc_uq UNIQUE (n))",
		),
		acceptance(capability.UniqueNullsDistinctClause,
			[]string{"CREATE TABLE ndc (n int)"},
			"CREATE UNIQUE INDEX ndc_uq ON ndc (n) NULLS NOT DISTINCT",
		),
		acceptance(capability.DeferrableConstraints,
			[]string{
				"CREATE TABLE dfc_parent (id int PRIMARY KEY)",
				"CREATE TABLE dfc_child (parent_id int)",
			},
			"ALTER TABLE dfc_child ADD CONSTRAINT dfc_c FOREIGN KEY (parent_id) "+
				"REFERENCES dfc_parent (id) DEFERRABLE INITIALLY DEFERRED",
		),
		// The read-back, not the acceptance. See the package comment.
		storedNotNullName(nil,
			"CREATE TABLE nnc (n int CONSTRAINT nnc_nn NOT NULL)",
			"SELECT COUNT(*) FROM sys.objects WHERE name = 'nnc_nn'",
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
		// T-SQL writes a recursive common table expression with no RECURSIVE
		// keyword, so this statement is refused for the word rather than for
		// the catalog. Both halves of the key are absent here and the plan
		// sends the spelling the key names.
		acceptance(capability.CatalogRecursiveCTE, nil,
			"WITH RECURSIVE m AS (SELECT relname FROM pg_class) SELECT COUNT(*) FROM m",
		),
		acceptance(capability.CatalogViewDependencies, nil,
			"SELECT COUNT(*) FROM information_schema.view_table_usage",
		),
		acceptance(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints WHERE 1 = 0",
		),
		sqlServerDDLInsideTransaction(),
	}

	return plan{experiments: experiments, undecided: map[capability.Capability]string{
		capability.CatalogVectorInfo: "ALL_TAB_COLS.VECTOR_INFO is an Oracle catalog column; this server has no such " +
			"relation, and the reader the key gates runs only against Oracle, so neither " +
			"having nor lacking it here would decide the key",
		capability.RowDeletionPolicy: "the key names a table clause Ptah renders, reads and plans only " +
			"for Spanner, whose PostgreSQL interface stores it; T-SQL has no such clause, so a refusal " +
			"would answer a different question",
		capability.Hypertables: "create_hypertable is a TimescaleDB function, and TimescaleDB is a PostgreSQL " +
			"extension SQL Server has no spelling of; its refusal would answer a different question",
		capability.ContinuousAggregates: "a TimescaleDB continuous aggregate is a PostgreSQL materialized " +
			"view with an extension option, which SQL Server has no spelling of",
		capability.TransactionalDDL: "the key names whether a failed migration rolls back as a unit, which one " +
			"accepted statement cannot show; it is decided by the engine's DDL semantics rather than by a " +
			"statement. This server does roll a CREATE TABLE back, and whether the migrator can wrap a " +
			"migration in that is what stokaro/ptah#3192 owns",
		capability.MigrationTimeouts: "the key names a runtime policy the migrator applies around a migration, not a " +
			"statement this probe can send",
		capability.ShowRoutinePrivilege: "the key names a MySQL global privilege. The probe connects as one account and " +
			"cannot ask whether a privilege exists without granting it, and SQL Server has no SHOW_ROUTINE at all",
	}}
}

// sqlServerRowLevelSecurity decides RowLevelSecurity by asking the question in
// this engine's own vocabulary.
//
// The key promises that a declared policy is rendered, read back and compared,
// not that the object is spelled the way PostgreSQL spells it. SQL Server has
// no table-level switch to enable and no inline predicate expression: a
// SECURITY POLICY names an inline table-valued function, so the function has to
// exist before the policy that invokes it. Sending PostgreSQL's `ALTER TABLE
// ... ENABLE ROW LEVEL SECURITY` here would have recorded a refusal of a
// statement Ptah never emits for this target.
func sqlServerRowLevelSecurity() experiment {
	return all(capability.RowLevelSecurity,
		[]string{"CREATE TABLE rls (n int)"},
		"CREATE FUNCTION rls_pred (@n int) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS allowed WHERE @n > 0",
		"CREATE SECURITY POLICY rls_pol ADD FILTER PREDICATE dbo.rls_pred(n) ON dbo.rls WITH (STATE = ON)",
	)
}

// sqlServerDDLInsideTransaction decides whether the server takes a schema
// statement inside an explicit transaction block.
//
// The other plans declare this key undecidable, because on those engines the
// answer belongs to the wrapper the migrator opens rather than to a statement.
// Here the server answers it: T-SQL has an explicit BEGIN TRANSACTION, so the
// block the key names is something the probe can actually put a statement
// inside of, and the ROLLBACK at the end leaves the table as the setup made it.
//
// It decides the narrow key only. Whether a failed migration rolls back as a
// unit is capability.TransactionalDDL, which this plan declares undecidable
// beside it.
func sqlServerDDLInsideTransaction() experiment {
	return experiment{
		decides: []capability.Capability{capability.DDLInsideTransaction},
		setup:   []string{"CREATE TABLE dit (n int)"},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempts, started := s.tryInTransaction(ctx, "ALTER TABLE dit ADD m int")
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

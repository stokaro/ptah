package capabilityprobe

import (
	"context"

	"go.5x5.cz/ptah/core/platform/capability"
)

// oraclePlan is the statement table for Oracle.
//
// Three of its experiments use [all] rather than [acceptance], and the reason
// is the sharpest thing this table records: on Oracle, ACCEPTED does not mean
// CREATED. Measured on 23.26, `CREATE TYPE e AS ENUM ('a','b')`,
// `CREATE TYPE r AS RANGE (SUBTYPE = NUMBER)` and the PostgreSQL composite
// spelling are all accepted, and all three produce an OBJECT type with zero
// attributes that cannot be used as a column type -- Oracle parses a forward
// type declaration and the statement succeeds. An acceptance experiment would
// have scored all three true. The second statement, which USES the type, is
// what decides them.
//
// Every statement here was run against gvenzl/oracle-free:slim 23.26.2.0.0 and
// gvenzl/oracle-xe:21-slim 21.3.0.0.0 while stokaro/ptah#1875 was written.
func oraclePlan() plan {
	experiments := []experiment{
		acceptance(capability.DropConstraintGeneric,
			[]string{"CREATE TABLE dcg (n NUMBER(10), CONSTRAINT dcg_ck CHECK (n > 0))"},
			"ALTER TABLE dcg DROP CONSTRAINT dcg_ck",
		),
		// Refused inside ALTER on both lines -- ORA-01735 -- on the same server
		// that takes the guard on CREATE TABLE and DROP INDEX. That split is
		// what this key names.
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{"CREATE TABLE dcie (n NUMBER(10))"},
			decide: guarded(capability.DropConstraintIfExists, nil,
				[]string{"ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent"},
				"ALTER TABLE dcie DROP CONSTRAINT dcie_absent",
			).decide,
		},
		guarded(capability.DropIndexIfExists, nil,
			[]string{"DROP INDEX IF EXISTS dii_absent"},
			"DROP INDEX dii_absent",
		),
		guarded(capability.ObjectExistenceGuards, nil,
			[]string{"DROP TABLE IF EXISTS oeg_absent"},
			"DROP TABLE oeg_absent",
		),
		enforced(capability.CheckConstraintsEnforced,
			[]string{"CREATE TABLE cce (n NUMBER(10), CONSTRAINT cce_ck CHECK (n > 0))"},
			"INSERT INTO cce (n) VALUES (1)",
			"INSERT INTO cce (n) VALUES (-1)",
		),
		acceptance(capability.DropCheckClause,
			[]string{"CREATE TABLE dcc (n NUMBER(10), CONSTRAINT dcc_ck CHECK (n > 0))"},
			"ALTER TABLE dcc DROP CHECK dcc_ck",
		),
		acceptance(capability.EnumInlineColumn, nil,
			"CREATE TABLE eic (c ENUM('a','b'))",
		),
		// The second statement is the experiment. See the package comment.
		all(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a','b')",
			"CREATE TABLE ect_t (c ect)",
		),
		// AS OBJECT rather than PostgreSQL's AS, because the plan measures the
		// statement Ptah RENDERS and that is the one it renders. The
		// distinction is not cosmetic and this pair is what shows it: the
		// PostgreSQL spelling is ACCEPTED here and leaves an INCOMPLETE type
		// with no attributes, so the first statement passed and the second
		// answered that the column's type does not exist -- which is the plan
		// correctly reporting that Ptah could not have used it
		// (stokaro/ptah#1920).
		all(capability.CompositeTypes, nil,
			"CREATE TYPE cpt AS OBJECT (a NUMBER(10), b VARCHAR2(10))",
			"CREATE TABLE cpt_t (c cpt)",
		),
		all(capability.RangeTypes, nil,
			"CREATE TYPE rgt AS RANGE (SUBTYPE = NUMBER)",
			"CREATE TABLE rgt_t (c rgt)",
		),
		all(capability.DomainTypes, nil,
			"CREATE DOMAIN dmt AS NUMBER(10) NOT NULL",
			"CREATE TABLE dmt_t (c dmt)",
		),
		acceptance(capability.CreateIndexConcurrently,
			[]string{"CREATE TABLE cic (n NUMBER(10))"},
			"CREATE INDEX CONCURRENTLY cic_ix ON cic (n)",
		),
		acceptance(capability.DropIndexConcurrently,
			[]string{"CREATE TABLE dic (n NUMBER(10))", "CREATE INDEX dic_ix ON dic (n)"},
			"DROP INDEX CONCURRENTLY dic_ix",
		),
		acceptance(capability.Views,
			[]string{"CREATE TABLE vsrc (n NUMBER(10))"},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		acceptance(capability.MaterializedViews,
			[]string{"CREATE TABLE mvsrc (n NUMBER(10))"},
			"CREATE MATERIALIZED VIEW mvw AS SELECT n FROM mvsrc",
		),
		acceptance(capability.Functions, nil,
			"CREATE OR REPLACE FUNCTION fnc RETURN NUMBER IS BEGIN RETURN 1; END;",
		),
		acceptance(capability.Procedures, nil,
			"CREATE OR REPLACE PROCEDURE prc IS BEGIN NULL; END;",
		),
		all(capability.Triggers,
			[]string{"CREATE TABLE trg_t (n NUMBER(10))"},
			"CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW BEGIN NULL; END;",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup:    []string{"CREATE TABLE cort_t (n NUMBER(10))"},
			decide: all(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW BEGIN NULL; END;",
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW BEGIN NULL; END;",
			).decide,
		},
		acceptance(capability.GeneratedColumns, nil,
			"CREATE TABLE gcl (n NUMBER(10), g NUMBER(10) GENERATED ALWAYS AS (n * 2) VIRTUAL)",
		),
		{
			// The setup IS the generated-column clause, so a target without it
			// cannot be asked this question at all.
			decides:  []capability.Capability{capability.AlterGeneratedColumnExpression},
			requires: []capability.Capability{capability.GeneratedColumns},
			setup:    []string{"CREATE TABLE agc (n NUMBER(10), g NUMBER(10) GENERATED ALWAYS AS (n + 1) VIRTUAL)"},
			decide: acceptance(capability.AlterGeneratedColumnExpression, nil,
				"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
			).decide,
		},
		acceptance(capability.RowLevelSecurity,
			[]string{"CREATE TABLE rls (n NUMBER(10))"},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
		),
		oracleRoleManagement(),
		acceptance(capability.ForeignKeys,
			[]string{
				"CREATE TABLE fk_parent (id NUMBER(10) PRIMARY KEY)",
				"CREATE TABLE fk_child (parent_id NUMBER(10))",
			},
			"ALTER TABLE fk_child ADD CONSTRAINT fk_c FOREIGN KEY (parent_id) REFERENCES fk_parent (id)",
		),
		// The three reference-policy keys are one mutual-exclusion group, and
		// two statements answer all three consistently. Asking them separately
		// could produce a combination no preset is allowed to hold.
		referencePolicy(referencePolicyStatements{
			setup: []string{
				"CREATE TABLE rp_unique (id NUMBER(10) PRIMARY KEY)",
				"CREATE TABLE rp_indexed (v NUMBER(10))",
				"CREATE INDEX rp_indexed_ix ON rp_indexed (v)",
				"CREATE TABLE rp_bare (v NUMBER(10))",
				"CREATE TABLE rp_child (u NUMBER(10), i NUMBER(10), b NUMBER(10))",
			},
			unique:  "ALTER TABLE rp_child ADD CONSTRAINT rp_u FOREIGN KEY (u) REFERENCES rp_unique (id)",
			indexed: "ALTER TABLE rp_child ADD CONSTRAINT rp_i FOREIGN KEY (i) REFERENCES rp_indexed (v)",
			bare:    "ALTER TABLE rp_child ADD CONSTRAINT rp_b FOREIGN KEY (b) REFERENCES rp_bare (v)",
		}),
		acceptance(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
		),
		// The restriction Spanner has: a bare CREATE SEQUENCE is accepted and
		// an option clause beside it is not. The bare form is the control, so a
		// server that refuses every CREATE SEQUENCE reads as undecidable rather
		// than as restricted. Oracle takes the whole clause, measured.
		enforced(capability.SequenceStartCounterOnly, nil,
			"CREATE SEQUENCE sso_ok",
			"CREATE SEQUENCE sso_no INCREMENT BY 2",
		),
		acceptance(capability.XMLType, nil,
			"CREATE TABLE xmlt (c XMLTYPE)",
		),
		acceptance(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1) FROM dual",
		),
		acceptance(capability.RowLevelTTL,
			[]string{"CREATE TABLE ttl (n NUMBER(10))"},
			"ALTER TABLE ttl SET (ttl_expire_after = '1 day')",
		),
		acceptance(capability.CheckGrantStatement,
			[]string{"CREATE TABLE cgs (n NUMBER(10))"},
			"CHECK GRANT SELECT ON cgs",
		),
		acceptance(capability.RenameColumnClause,
			[]string{"CREATE TABLE rcc (n NUMBER(10))"},
			"ALTER TABLE rcc RENAME COLUMN n TO m",
		),
		acceptance(capability.DeferrableConstraints,
			[]string{
				"CREATE TABLE dfc_parent (id NUMBER(10) PRIMARY KEY)",
				"CREATE TABLE dfc_child (parent_id NUMBER(10))",
			},
			"ALTER TABLE dfc_child ADD CONSTRAINT dfc_c FOREIGN KEY (parent_id) "+
				"REFERENCES dfc_parent (id) DEFERRABLE INITIALLY DEFERRED",
		),
		acceptance(capability.IndexIncludeSPGiST,
			[]string{"CREATE TABLE spg (n NUMBER(10), m NUMBER(10))"},
			"CREATE INDEX spg_ix ON spg USING SPGIST (n) INCLUDE (m)",
		),
		acceptance(capability.PostgresCatalogFunctions, nil,
			"SELECT obj_description(1) FROM dual",
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
		acceptance(capability.CatalogRecursiveCTE, nil,
			"WITH m AS (SELECT relname FROM pg_class) SELECT COUNT(*) FROM m",
		),
		acceptance(capability.CatalogViewDependencies, nil,
			"SELECT COUNT(*) FROM information_schema.view_table_usage",
		),
		acceptance(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints WHERE 1 = 0",
		),
		acceptance(capability.DDLInsideTransaction,
			[]string{"CREATE TABLE dit (n NUMBER(10))"},
			"ALTER TABLE dit ADD (m NUMBER(10))",
		),
	}

	return plan{experiments: experiments, undecided: map[capability.Capability]string{
		capability.RowDeletionPolicy: "the key names a table clause Ptah renders, reads and plans only " +
			"for Spanner, whose PostgreSQL interface stores it; this server has no such clause, so its " +
			"refusal would answer a different question",
		capability.NamedNotNullConstraints: "the key names a PostgreSQL 18 catalog behavior; this " +
			"server names no NOT NULL constraint at all, so its answer would be to a different question",
		capability.Hypertables: "create_hypertable is a TimescaleDB function, and TimescaleDB is a PostgreSQL " +
			"extension Oracle has no spelling of; its refusal would answer a different question",
		capability.ContinuousAggregates: "a TimescaleDB continuous aggregate is a PostgreSQL materialized " +
			"view with an extension option, which Oracle has no spelling of",
		// A schema statement Oracle commits by itself: measured, a CREATE TABLE
		// inside an explicit transaction survives ROLLBACK on both lines. That
		// is a property of the engine's DDL semantics rather than of any single
		// statement this probe can send, which is what every other plan says
		// about this key too.
		capability.TransactionalDDL: "the key names whether a failed migration rolls back as a unit, which one accepted " +
			"statement cannot show; it is decided by the engine's DDL semantics rather than by a statement",
		capability.MigrationTimeouts: "the key names a runtime policy the migrator applies around a migration, not a " +
			"statement this probe can send",
		capability.ShowRoutinePrivilege: "the key names a MySQL global privilege. The probe connects as one account and " +
			"cannot ask whether a privilege exists without granting it, and Oracle has no SHOW_ROUTINE at all",
	}}
}

// oracleRoleManagement decides RoleManagement and registers the role it created
// for cleanup.
//
// A role is database-scoped in Oracle, so DROP USER ... CASCADE does not remove
// it. Measured: a run that forgot one made the NEXT run answer
// `ORA-01921: role name 'RLM' conflicts with another user or role name` -- and
// that refusal read as the engine not supporting roles, which happened to match
// the preset. A key agreeing for the wrong reason is worse than one that
// disagrees, because nothing looks wrong.
func oracleRoleManagement() experiment {
	const role = "ptah_capprobe_role"
	return experiment{
		decides: []capability.Capability{capability.RoleManagement},
		setup:   []string{"CREATE TABLE rm_t (n NUMBER(10))"},
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			created := s.exec(ctx, "CREATE ROLE "+role)
			attempts := []Attempt{created}
			if !created.Accepted {
				return verdicts{capability.RoleManagement: decided(false)}, attempts
			}
			s.roles = append(s.roles, role)
			granted := s.exec(ctx, "GRANT SELECT ON rm_t TO "+role)
			attempts = append(attempts, granted)
			return verdicts{capability.RoleManagement: decided(granted.Accepted)}, attempts
		},
	}
}

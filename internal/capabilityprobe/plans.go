package capabilityprobe

import (
	"context"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// planFor returns the experiment table for a dialect, or false when the probe
// has none.
//
// A dialect with no plan is not a dialect that agrees with its preset. Every
// one of its rows is undecidable with that stated, which is the honest reading
// of "nobody wrote a decider for this yet".
func planFor(dialect string) (plan, bool) {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return postgresPlan(), true
	case platform.MySQL, platform.MariaDB:
		return mysqlFamilyPlan(platform.NormalizeDialect(dialect)), true
	default:
		return plan{}, false
	}
}

// postgresPlan is the statement table for the PostgreSQL wire family.
//
// Every object name is unqualified so it lands in the throwaway schema the
// session entered, and every experiment creates its own objects rather than
// borrowing another's: an experiment that depends on a neighbor's setup
// reports that neighbor's failure as its own answer.
func postgresPlan() plan {
	experiments := []experiment{
		acceptance(capability.DropConstraintGeneric,
			[]string{"CREATE TABLE dcg (n int, CONSTRAINT dcg_uq UNIQUE (n))"},
			"ALTER TABLE dcg DROP CONSTRAINT dcg_uq",
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
		guarded(capability.DropIndexIfExists, nil,
			[]string{"DROP INDEX IF EXISTS dii_absent"},
			"DROP INDEX dii_absent",
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
		acceptance(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a','b')",
		),
		concurrentIndex(capability.CreateIndexConcurrently,
			[]string{"CREATE TABLE cic (n int)"},
			"CREATE INDEX CONCURRENTLY cic_one ON cic (n)",
			"CREATE INDEX CONCURRENTLY cic_two ON cic (n)",
		),
		concurrentIndex(capability.DropIndexConcurrently,
			[]string{
				"CREATE TABLE dic (n int)",
				"CREATE INDEX dic_one ON dic (n)",
				"CREATE INDEX dic_two ON dic (n)",
			},
			"DROP INDEX CONCURRENTLY dic_one",
			"DROP INDEX CONCURRENTLY dic_two",
		),
		acceptance(capability.Views,
			[]string{"CREATE TABLE vsrc (n int)"},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		storedResult(capability.MaterializedViews,
			[]string{"CREATE TABLE mvs (n int)"},
			"CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs",
			"SELECT c FROM mvw",
			"INSERT INTO mvs (n) VALUES (1)",
		),
		acceptance(capability.Functions, nil,
			"CREATE FUNCTION fn() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		),
		all(capability.Triggers,
			[]string{"CREATE TABLE trg_t (n int)"},
			"CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
			"CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_fn()",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup: []string{
				"CREATE TABLE cort_t (n int)",
				"CREATE FUNCTION cort_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
			},
			decide: acceptance(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW EXECUTE FUNCTION cort_fn()",
			).decide,
		},
		acceptance(capability.AlterGeneratedColumnExpression,
			[]string{"CREATE TABLE agc (n int, g int GENERATED ALWAYS AS (n + 1) STORED)"},
			"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
		),
		all(capability.RowLevelSecurity,
			[]string{"CREATE TABLE rls (n int)"},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
			"CREATE POLICY rls_p ON rls USING (true)",
		),
		roleManagement(),
		foreignKeys(
			[]string{
				"CREATE TABLE fk_parent (id int PRIMARY KEY)",
				"CREATE TABLE fk_child (id int)",
			},
			"ALTER TABLE fk_child ADD CONSTRAINT fk_child_fk FOREIGN KEY (id) REFERENCES fk_parent (id)",
			"INSERT INTO fk_parent (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (99)",
		),
		referencePolicy(referencePolicyStatements{
			setup: []string{
				"CREATE TABLE fkp_uni (k int NOT NULL, CONSTRAINT fkp_uni_uq UNIQUE (k))",
				"CREATE TABLE fkp_idx (k int NOT NULL)",
				"CREATE INDEX fkp_idx_k ON fkp_idx (k)",
				"CREATE TABLE fkp_none (k int NOT NULL)",
				"CREATE TABLE fkp_c0 (k int)",
				"CREATE TABLE fkp_c1 (k int)",
				"CREATE TABLE fkp_c2 (k int)",
			},
			unique:  "ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k)",
			indexed: "ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k)",
			bare:    "ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k)",
		}),
		all(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
			"CREATE TABLE ser (id SERIAL PRIMARY KEY)",
		),
		acceptance(capability.XMLType, nil,
			"CREATE TABLE xmlt (c XML)",
		),
		all(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1)",
			"SELECT pg_advisory_unlock(1)",
		),
	}
	return plan{experiments: experiments, undecided: map[capability.Capability]string{}}
}

// mysqlFamilyPlan is the statement table for MySQL and MariaDB.
func mysqlFamilyPlan(dialect string) plan {
	experiments := []experiment{
		acceptance(capability.DropConstraintGeneric,
			[]string{"CREATE TABLE dcg (n int, CONSTRAINT dcg_uq UNIQUE (n))"},
			"ALTER TABLE dcg DROP CONSTRAINT dcg_uq",
		),
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{"CREATE TABLE dcie (n int)"},
			// The key names two spellings, and a probe that tests only the
			// first reports the row as agreeing while half of what the flag
			// gates went unmeasured: the MySQL-family renderer puts the same
			// guard on the FOREIGN KEY branch.
			decide: guarded(capability.DropConstraintIfExists, nil,
				[]string{
					"ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent",
					"ALTER TABLE dcie DROP FOREIGN KEY IF EXISTS dcie_absent_fk",
				},
				"ALTER TABLE dcie DROP CONSTRAINT dcie_absent",
			).decide,
		},
		guarded(capability.DropIndexIfExists,
			[]string{"CREATE TABLE dii (n int)"},
			[]string{"DROP INDEX IF EXISTS dii_absent ON dii"},
			"DROP INDEX dii_absent ON dii",
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
		acceptance(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a','b')",
		),
		concurrentIndex(capability.CreateIndexConcurrently,
			[]string{"CREATE TABLE cic (n int)"},
			"CREATE INDEX CONCURRENTLY cic_one ON cic (n)",
			"CREATE INDEX CONCURRENTLY cic_two ON cic (n)",
		),
		concurrentIndex(capability.DropIndexConcurrently,
			[]string{
				"CREATE TABLE dic (n int)",
				"CREATE INDEX dic_one ON dic (n)",
				"CREATE INDEX dic_two ON dic (n)",
			},
			"DROP INDEX CONCURRENTLY dic_one ON dic",
			"DROP INDEX CONCURRENTLY dic_two ON dic",
		),
		acceptance(capability.Views,
			[]string{"CREATE TABLE vsrc (n int)"},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		storedResult(capability.MaterializedViews,
			[]string{"CREATE TABLE mvs (n int)"},
			"CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs",
			"SELECT c FROM mvw",
			"INSERT INTO mvs (n) VALUES (1)",
		),
		acceptance(capability.Functions, nil,
			"CREATE FUNCTION fn() RETURNS INT DETERMINISTIC RETURN 1",
		),
		acceptance(capability.Triggers,
			[]string{"CREATE TABLE trg_t (n int)"},
			"CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW SET NEW.n = NEW.n",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup:    []string{"CREATE TABLE cort_t (n int)"},
			decide: acceptance(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW SET NEW.n = NEW.n",
			).decide,
		},
		acceptanceNote(capability.AlterGeneratedColumnExpression,
			[]string{"CREATE TABLE agc (n int, g int GENERATED ALWAYS AS (n + 1) STORED)"},
			"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
			"this key names PostgreSQL 17's SET EXPRESSION spelling, which is what was probed; "+
				"the MySQL family can change a generated column expression under its own "+
				"MODIFY COLUMN spelling, so a false row here means \"not this statement\", not \"not this ability\"",
		),
		all(capability.RowLevelSecurity,
			[]string{"CREATE TABLE rls (n int)"},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
			"CREATE POLICY rls_p ON rls USING (true)",
		),
		foreignKeys(
			[]string{
				"CREATE TABLE fk_parent (id int PRIMARY KEY)",
				"CREATE TABLE fk_child (id int)",
			},
			"ALTER TABLE fk_child ADD CONSTRAINT fk_child_fk FOREIGN KEY (id) REFERENCES fk_parent (id)",
			"INSERT INTO fk_parent (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (99)",
		),
		referencePolicy(referencePolicyStatements{
			setup: []string{
				"CREATE TABLE fkp_uni (k int NOT NULL, CONSTRAINT fkp_uni_uq UNIQUE (k))",
				"CREATE TABLE fkp_idx (k int NOT NULL)",
				"CREATE INDEX fkp_idx_k ON fkp_idx (k)",
				"CREATE TABLE fkp_none (k int NOT NULL)",
				"CREATE TABLE fkp_c0 (k int)",
				"CREATE TABLE fkp_c1 (k int)",
				"CREATE TABLE fkp_c2 (k int)",
			},
			unique:  "ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k)",
			indexed: "ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k)",
			bare:    "ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k)",
		}),
		acceptance(capability.XMLType, nil,
			"CREATE TABLE xmlt (c XML)",
		),
		all(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1)",
			"SELECT pg_advisory_unlock(1)",
		),
	}

	undecided := map[capability.Capability]string{
		// Measured live on MySQL 9.7.1: CREATE ROLE and GRANT SELECT are both
		// accepted at exit 0 while MySQL84 records this key false. The two are
		// not in conflict — the key names the PostgreSQL role and object
		// privilege surface that the PostgreSQL planner, renderer and reader
		// gate on, and no MySQL-family code path reads it. An acceptance probe
		// here would manufacture exactly the false disagreement this harness
		// exists to avoid.
		capability.RoleManagement: "the key names the PostgreSQL role and privilege surface no MySQL-family " +
			"code path consults; this server's own CREATE ROLE and GRANT are a different surface, " +
			"so accepting them would not decide the key",
	}
	if dialect == platform.MariaDB {
		// MariaDB has had SEQUENCE objects since 10.3 and the preset still
		// says false, deliberately: the key describes Ptah's generator, and
		// there is no MariaDB sequence introspection and no MySQL-family
		// sequence planning. The two answers are known in advance to differ on
		// this dialect, so the engine cannot decide the key.
		undecided[capability.Sequences] = "the key describes Ptah's generator rather than the engine " +
			"(stokaro/ptah#931 item 8): MariaDB has had SEQUENCE since 10.3 while no MySQL-family " +
			"renderer or planner emits, reads or plans one, so the server's answer is to a different question"
	} else {
		experiments = append(experiments, all(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
			"CREATE TABLE ser (id SERIAL PRIMARY KEY)",
		))
	}
	return plan{experiments: experiments, undecided: undecided}
}

// roleManagement decides RoleManagement on the PostgreSQL family and registers
// the role it created for cleanup: a role is cluster-scoped, so dropping the
// probe schema does not remove it.
func roleManagement() experiment {
	const role = "ptah_capprobe_role"
	return experiment{
		decides: []capability.Capability{capability.RoleManagement},
		setup:   []string{"CREATE TABLE rm_t (n int)"},
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

// foreignKeys decides ForeignKeys.
//
// DDL acceptance is not enough: SQLite parses a foreign key clause and does not
// enforce it unless PRAGMA foreign_keys is on for the connection, and a MySQL
// table on a non-InnoDB engine has the same parse-and-ignore shape. So the
// constraint has to bite — an orphan row must be refused — and a row that
// satisfies it must be accepted, or a table refusing every insert would read
// as an enforced foreign key.
func foreignKeys(setup []string, add, seedParent, validChild, orphanChild string) experiment {
	return experiment{
		decides: []capability.Capability{capability.ForeignKeys},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			added := s.exec(ctx, add)
			attempts := []Attempt{added}
			if !added.Accepted {
				return verdicts{capability.ForeignKeys: decided(false)}, attempts
			}
			seeded := s.exec(ctx, seedParent)
			valid := s.exec(ctx, validChild)
			orphan := s.exec(ctx, orphanChild)
			attempts = append(attempts, seeded, valid, orphan)
			if !seeded.Accepted || !valid.Accepted {
				return verdicts{capability.ForeignKeys: cannotDecide(
					"the constraint was accepted but the control rows were refused (%s), "+
						"so refusing the orphan row would not show the constraint is enforced",
					collapse(firstError(seeded, valid)),
				)}, attempts
			}
			return verdicts{capability.ForeignKeys: decided(!orphan.Accepted)}, attempts
		},
	}
}

func firstError(attempts ...Attempt) string {
	for _, attempt := range attempts {
		if !attempt.Accepted {
			return attempt.ServerErr
		}
	}
	return ""
}

// referencePolicyStatements is the two-statement experiment that decides the
// whole foreign-key reference policy group.
type referencePolicyStatements struct {
	setup []string
	// unique adds a foreign key to a declared unique key. It is the control:
	// a server that refuses even this one is refusing foreign keys, not
	// choosing a reference policy.
	unique string
	// indexed adds a foreign key to a nonunique but indexed key.
	indexed string
	// bare adds a foreign key to a column with no index at all.
	bare string
}

// referencePolicy decides ForeignKeysRequireUniqueReference,
// ForeignKeysRequireIndexedReference and ForeignKeysCreateBackingIndex.
//
// These are one mutual-exclusion group and Validate accepts exactly one of them
// enabled. Three independent experiments could therefore produce a combination
// no preset is allowed to hold; two statements produce all three answers
// consistently:
//
//	indexed refused                  -> the target requires a unique key
//	indexed accepted, bare refused   -> the target requires an indexed key
//	indexed accepted, bare accepted  -> the target builds the backing index
func referencePolicy(stmts referencePolicyStatements) experiment {
	keys := []capability.Capability{
		capability.ForeignKeysRequireUniqueReference,
		capability.ForeignKeysRequireIndexedReference,
		capability.ForeignKeysCreateBackingIndex,
	}
	return experiment{
		decides:  keys,
		requires: []capability.Capability{capability.ForeignKeys},
		setup:    stmts.setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			control := s.exec(ctx, stmts.unique)
			attempts := []Attempt{control}
			if !control.Accepted {
				reason := cannotDecide(
					"the server refused a foreign key to a declared unique key as well (%s), "+
						"so refusing the other two says nothing about which reference policy it uses",
					collapse(control.ServerErr),
				)
				return verdicts{keys[0]: reason, keys[1]: reason, keys[2]: reason}, attempts
			}
			indexed := s.exec(ctx, stmts.indexed)
			bare := s.exec(ctx, stmts.bare)
			attempts = append(attempts, indexed, bare)
			return verdicts{
				keys[0]: decided(!indexed.Accepted),
				keys[1]: decided(indexed.Accepted && !bare.Accepted),
				keys[2]: decided(indexed.Accepted && bare.Accepted),
			}, attempts
		},
	}
}

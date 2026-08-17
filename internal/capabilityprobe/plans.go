package capabilityprobe

import (
	"context"
	"fmt"

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
		return postgresFamilyPlan(platform.NormalizeDialect(dialect)), true
	case platform.MySQL, platform.MariaDB:
		return mysqlFamilyPlan(platform.NormalizeDialect(dialect)), true
	case platform.ClickHouse:
		return clickHousePlan(), true
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
// spannerSpelling writes the same experiments in the DDL Spanner accepts.
//
// Spanner reaches Ptah over the PostgreSQL WIRE and does not speak PostgreSQL
// DDL. Two differences reach every experiment that needs a throwaway table, and
// both were measured against a live endpoint rather than read anywhere:
//
//	CREATE TABLE rm_t (n int)  -> Primary key must be defined for table "rm_t"
//	CONSTRAINT dcg_uq UNIQUE   -> <UNIQUE> constraint is not supported, create a unique index instead
//
// The first is mechanical. The second is not: spelling the unique constraint as
// an index would make the drop-a-constraint experiment ask about an index, and
// answer "Spanner cannot drop a constraint" on a server that can drop the CHECK
// constraint it does have. So the spelling carries the experiment's MEANING --
// "a table with a droppable named constraint" -- and each dialect picks a
// constraint kind it actually supports (stokaro/ptah#942).
var spannerSpelling = tableSpelling{keyed: true, dropCheckConstraint: true}

// tableSpelling writes throwaway tables for one dialect. The zero value is the
// PostgreSQL spelling, which is what Postgres, CockroachDB and YugabyteDB have
// always been measured with.
type tableSpelling struct {
	// keyed adds a primary key to every table that declares none.
	keyed bool
	// dropCheckConstraint picks CHECK for the droppable-constraint experiment,
	// on a dialect with no UNIQUE table constraint to drop.
	dropCheckConstraint bool
	// engine is the storage clause a dialect requires after the column list,
	// with %s standing for the key column. ClickHouse has no default engine for
	// a CREATE TABLE and refuses one without it.
	engine string
}

// table spells a throwaway table. key names the column that becomes the primary
// key on a dialect that requires one, and is unused where it does not.
func (t tableSpelling) table(name, columns, key string) string {
	if t.engine != "" {
		return fmt.Sprintf("CREATE TABLE %s (%s) %s", name, columns, fmt.Sprintf(t.engine, key))
	}
	if !t.keyed {
		return fmt.Sprintf("CREATE TABLE %s (%s)", name, columns)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))", name, columns, key)
}

// droppableConstraint spells a table carrying one named constraint this dialect
// can drop, and the statement that drops it. The constraint KIND is the
// dialect's choice; what the experiment decides is whether a named constraint
// can be dropped generically, not which kind was available to try it on.
func (t tableSpelling) droppableConstraint(table, constraint string) (setup []string, drop string) {
	clause := fmt.Sprintf("CONSTRAINT %s UNIQUE (n)", constraint)
	if t.dropCheckConstraint {
		clause = fmt.Sprintf("CONSTRAINT %s CHECK (n > 0)", constraint)
	}
	return []string{t.table(table, "n int, "+clause, "n")},
		fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", table, constraint)
}

// uniquelyReferenced spells a table whose column can be the target of a foreign
// key, using the unique index a dialect without a UNIQUE table constraint
// requires -- which is what that dialect's own refusal recommends.
func (t tableSpelling) uniquelyReferenced(table, constraint, column string) []string {
	if !t.dropCheckConstraint {
		return []string{t.table(table, fmt.Sprintf("%s int NOT NULL, CONSTRAINT %s UNIQUE (%s)", column, constraint, column), column)}
	}
	return []string{
		t.table(table, column+" int NOT NULL", column),
		fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s)", constraint, table, column),
	}
}

// postgresFamilyPlan is the statement table for the PostgreSQL wire family.
//
// The experiments are one list for every dialect in it. What varies is how a
// throwaway table is spelled, which is a property of the dialect's DDL and not
// of the question being asked.
func postgresFamilyPlan(dialect string) plan {
	t := tableSpelling{}
	if platform.NormalizeDialect(dialect) == platform.Spanner {
		t = spannerSpelling
	}
	dcgSetup, dcgDrop := t.droppableConstraint("dcg", "dcg_uq")
	experiments := []experiment{
		acceptance(capability.DropConstraintGeneric, dcgSetup, dcgDrop),
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{t.table("dcie", "n int", "n")},
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
			[]string{t.table("cce", "n int, CONSTRAINT cce_ck CHECK (n > 0)", "n")},
			"INSERT INTO cce (n) VALUES (1)",
			"INSERT INTO cce (n) VALUES (-1)",
		),
		acceptance(capability.DropCheckClause,
			[]string{t.table("dcc", "n int, CONSTRAINT dcc_ck CHECK (n > 0)", "n")},
			"ALTER TABLE dcc DROP CHECK dcc_ck",
		),
		acceptance(capability.EnumInlineColumn, nil,
			"CREATE TABLE eic (c ENUM('a','b'))",
		),
		acceptance(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a','b')",
		),
		concurrentIndex(capability.CreateIndexConcurrently,
			[]string{t.table("cic", "n int", "n")},
			"CREATE INDEX CONCURRENTLY cic_one ON cic (n)",
			"CREATE INDEX CONCURRENTLY cic_two ON cic (n)",
		),
		concurrentIndex(capability.DropIndexConcurrently,
			[]string{
				t.table("dic", "n int", "n"),
				"CREATE INDEX dic_one ON dic (n)",
				"CREATE INDEX dic_two ON dic (n)",
			},
			"DROP INDEX CONCURRENTLY dic_one",
			"DROP INDEX CONCURRENTLY dic_two",
		),
		indexIncludeSPGiST(
			[]string{t.table("iis", "k text, payload int", "k")},
			"CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload)",
			`SELECT COUNT(*)
			 FROM pg_catalog.pg_index AS i
			 JOIN pg_catalog.pg_class AS idx ON idx.oid = i.indexrelid
			 JOIN pg_catalog.pg_am AS am ON am.oid = idx.relam
			 JOIN pg_catalog.pg_namespace AS ns ON ns.oid = idx.relnamespace
			 WHERE ns.nspname = current_schema()
			   AND idx.relname = 'iis_idx'
			   AND am.amname = 'spgist'
			   AND i.indnkeyatts = 1
			   AND i.indnatts = 2`,
		),
		acceptance(capability.Views,
			[]string{t.table("vsrc", "n int", "n")},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		storedResult(capability.MaterializedViews,
			[]string{t.table("mvs", "n int", "n")},
			"CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs",
			"SELECT c FROM mvw",
			"INSERT INTO mvs (n) VALUES (1)",
		),
		acceptance(capability.Functions, nil,
			"CREATE FUNCTION fn() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		),
		all(capability.Triggers,
			[]string{t.table("trg_t", "n int", "n")},
			"CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
			"CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_fn()",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup: []string{
				t.table("cort_t", "n int", "n"),
				"CREATE FUNCTION cort_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
			},
			decide: acceptance(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW EXECUTE FUNCTION cort_fn()",
			).decide,
		},
		// The three catalog keys below are decided by asking the catalog the
		// question Ptah's reader asks it. Measured on the Cloud Spanner
		// emulator through PGAdapter 0.55.2, which refuses all three while
		// PostgreSQL accepts them (stokaro/ptah#942).
		acceptanceNote(capability.PostgresCatalogFunctions, nil,
			"SELECT obj_description(2200, 'pg_namespace')",
			"pg_catalog's introspection helpers; Spanner answers "+
				"`The Postgres Type is not supported: name`",
		),
		acceptanceNote(capability.CatalogRowStatistics, nil,
			"SELECT 1 FROM pg_stat_all_tables LIMIT 1",
			"the planner statistics view; Spanner answers "+
				"`relation \"pg_stat_all_tables\" does not exist`",
		),
		acceptanceNote(capability.CatalogDependencies, nil,
			"SELECT 1 FROM pg_depend LIMIT 1",
			"the dependency table the user-defined-type read joins; naming the "+
				"type system instead was refuted by CockroachDB, which refuses "+
				"CREATE DOMAIN and accepts a composite type",
		),
		acceptance(capability.AlterGeneratedColumnExpression,
			[]string{t.table("agc", "n int, g int GENERATED ALWAYS AS (n + 1) STORED", "n")},
			"ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)",
		),
		all(capability.RowLevelSecurity,
			[]string{t.table("rls", "n int", "n")},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
			"CREATE POLICY rls_p ON rls USING (true)",
		),
		roleManagement(t),
		foreignKeys(
			[]string{
				"CREATE TABLE fk_parent (id int PRIMARY KEY)",
				t.table("fk_child", "id int", "id"),
			},
			"ALTER TABLE fk_child ADD CONSTRAINT fk_child_fk FOREIGN KEY (id) REFERENCES fk_parent (id)",
			"INSERT INTO fk_parent (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (1)",
			"INSERT INTO fk_child (id) VALUES (99)",
		),
		referencePolicy(referencePolicyStatements{
			setup: append(t.uniquelyReferenced("fkp_uni", "fkp_uni_uq", "k"),
				t.table("fkp_idx", "k int NOT NULL", "k"),
				"CREATE INDEX fkp_idx_k ON fkp_idx (k)",
				t.table("fkp_none", "k int NOT NULL", "k"),
				t.table("fkp_c0", "k int", "k"),
				t.table("fkp_c1", "k int", "k"),
				t.table("fkp_c2", "k int", "k"),
			),
			unique:  "ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k)",
			indexed: "ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k)",
			bare:    "ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k)",
		}),
		all(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
			"CREATE TABLE ser (id SERIAL PRIMARY KEY)",
		),
		acceptance(capability.XMLType, nil,
			t.table("xmlt", "c XML", "c"),
		),
		all(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1)",
			"SELECT pg_advisory_unlock(1)",
		),
		// The one key in this plan whose expected answer is FALSE on
		// ClickHouse's statement, asked here because the registry answers for
		// every dialect and a key nobody asks about is a key nobody measured.
		// A server without it answers with a syntax error, which is the same
		// answer this experiment records anywhere else.
		acceptanceNote(capability.CheckGrantStatement, nil,
			"CHECK GRANT SELECT ON *.*",
			"the statement is ClickHouse's; a server without it refuses the syntax",
		),
		// The catalog view MySQL added in 8.0.13, asked as a catalog question
		// rather than a version comparison. PostgreSQL, CockroachDB and SQL
		// Server have the SQL-standard view too; MariaDB, ClickHouse and
		// Spanner answer `Unknown table` or `does not exist`, all measured
		// (stokaro/ptah#916 item 3).
		acceptanceNote(capability.CatalogViewDependencies, nil,
			"SELECT 1 FROM information_schema.view_table_usage LIMIT 1",
			"the catalog naming the tables a view reads",
		),
		// The MariaDB extension to the standard CHECK_CONSTRAINTS view, asked
		// as a catalog question. Selecting the column IS the question: a
		// server without it answers `Unknown column` and one with it returns
		// rows, so no version comparison is involved (stokaro/ptah#916 item 3).
		acceptanceNote(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints LIMIT 1",
			"the CHECK_CONSTRAINTS column that names the declaring table",
		),
		// Renaming a column in place. The statement is the one SQLite's
		// renderer emits, asked here because the registry answers for every
		// dialect (stokaro/ptah#916 item 3).
		acceptance(capability.RenameColumnClause,
			[]string{t.table("rnc", "a int", "a"), t.table("rnc_t", "n int, b int", "n")},
			"ALTER TABLE rnc_t RENAME COLUMN b TO c",
		),
		// The one key in this plan whose expected answer is FALSE on
		// PostgreSQL itself, so the usual reading of a verdict is inverted:
		// a refusal here is PostgreSQL behaving as its preset says, and an
		// ACCEPTANCE is what marks a CockroachDB line.
		//
		// The statement is the reproducer stokaro/ptah#1027 was filed with. It
		// names ttl_expiration_expression rather than ttl_expire_after because
		// that is the parameter Ptah models: the server rewrites an interval
		// on the way in, which is why internal/crdbttl refuses the other
		// enabler instead of modeling it.
		storedRowTTL(nil,
			"CREATE TABLE ttlp (id int PRIMARY KEY, expires_at TIMESTAMPTZ) "+
				"WITH (ttl_expiration_expression = 'expires_at')",
			// This is the projection internal/dbschema/postgres reads, on
			// purpose: the probe should ask the question the reader asks, so a
			// server the reader could not read is one this key reports false
			// for. The Spanner PostgreSQL interface serves pg_class from a
			// shim over information_schema and refuses both `current_schema()`
			// and a text[] cast, so it lands in exactly that branch.
			//
			// The table name is unqualified because the probe runs in a
			// throwaway schema of its own and `ttlp` belongs to this
			// experiment alone.
			`SELECT COUNT(*)
			 FROM pg_catalog.pg_class
			 WHERE relname = 'ttlp'
			   AND COALESCE(array_to_json(reloptions)::text, '[]') LIKE '%ttl_expiration_expression%'`,
		),
	}
	return plan{experiments: experiments, undecided: map[capability.Capability]string{
		// The probe connects as ONE account and cannot ask whether a privilege
		// EXISTS without being able to grant it: a server that refuses
		// `GRANT SHOW_ROUTINE` because the privilege is unknown and one that
		// refuses it because the grantee is not there answer the same way to an
		// acceptance test, and this harness reads acceptance rather than error
		// text. The threshold is MySQL 8.0.20 and the ladder carries it
		// (stokaro/ptah#916 item 3).
		capability.ShowRoutinePrivilege: "the probe cannot ask whether a privilege exists without granting it, " +
			"and an acceptance test cannot separate an unknown privilege from an absent grantee",
	}}
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
		uninspectableIndexIncludeSPGiST(
			[]string{"CREATE TABLE iis (k VARCHAR(255), payload INT)"},
			"CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload)",
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
		// ClickHouse's statement, asked here because the registry answers for
		// every dialect and a key nobody asks about is a key nobody measured.
		// A server without it answers with a syntax error, which is the same
		// answer this experiment records anywhere else.
		acceptanceNote(capability.CheckGrantStatement, nil,
			"CHECK GRANT SELECT ON *.*",
			"the statement is ClickHouse's; a server without it refuses the syntax",
		),
		// The catalog view MySQL added in 8.0.13, asked as a catalog question
		// rather than a version comparison. PostgreSQL, CockroachDB and SQL
		// Server have the SQL-standard view too; MariaDB, ClickHouse and
		// Spanner answer `Unknown table` or `does not exist`, all measured
		// (stokaro/ptah#916 item 3).
		acceptanceNote(capability.CatalogViewDependencies, nil,
			"SELECT 1 FROM information_schema.view_table_usage LIMIT 1",
			"the catalog naming the tables a view reads",
		),
		// The MariaDB extension to the standard CHECK_CONSTRAINTS view, asked
		// as a catalog question. Selecting the column IS the question: a
		// server without it answers `Unknown column` and one with it returns
		// rows, so no version comparison is involved (stokaro/ptah#916 item 3).
		acceptanceNote(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints LIMIT 1",
			"the CHECK_CONSTRAINTS column that names the declaring table",
		),
		// Renaming a column in place. The MySQL family spells the table the
		// same way, so the statement is the same question.
		acceptance(capability.RenameColumnClause,
			[]string{"CREATE TABLE rnc_t (n int, b int)"},
			"ALTER TABLE rnc_t RENAME COLUMN b TO c",
		),
	}

	undecided := map[capability.Capability]string{
		// The probe connects as ONE account and cannot ask whether a privilege
		// EXISTS without being able to grant it: a server that refuses
		// `GRANT SHOW_ROUTINE` because the privilege is unknown and one that
		// refuses it because the grantee is not there answer the same way to an
		// acceptance test, and this harness reads acceptance rather than error
		// text. The threshold is MySQL 8.0.20 and the ladder carries it
		// (stokaro/ptah#916 item 3).
		capability.ShowRoutinePrivilege: "the probe cannot ask whether a privilege exists without granting it, " +
			"and an acceptance test cannot separate an unknown privilege from an absent grantee",
		// Measured live on MySQL 9.7.1: CREATE ROLE and GRANT SELECT are both
		// accepted at exit 0 while MySQL84 records this key false. The two are
		// not in conflict. The key does not name a SERVER's role syntax — it is
		// engine-neutral, and PostgreSQL and ClickHouse both carry it — it names
		// whether PTAH plans, renders, introspects and compares a declared role
		// and grant for this target, and no MySQL-family code path does any of
		// those. An acceptance probe here would manufacture exactly the false
		// disagreement this harness exists to avoid.
		capability.RoleManagement: "the key names whether Ptah plans, renders, reads and compares a declared " +
			"role and grant for this target, which no MySQL-family code path does; this server accepting " +
			"its own CREATE ROLE and GRANT is a different question, so it would not decide the key",
		capability.PostgresCatalogFunctions: "the key names pg_catalog's introspection helpers, which this " +
			"server does not have and no MySQL-family reader asks for; its absence here says nothing " +
			"about the PostgreSQL-family reader the key gates",
		capability.CatalogRowStatistics: "pg_stat_all_tables is a PostgreSQL statistics view this server " +
			"does not have and no MySQL-family reader consults; its own statistics live elsewhere and " +
			"answer a different question",
		capability.CatalogDependencies: "pg_depend is a PostgreSQL catalog relation this server does not " +
			"have and no MySQL-family reader joins; its absence here says nothing about the " +
			"PostgreSQL-family read the key gates",
		capability.RowLevelTTL: "the key names a table storage parameter Ptah renders, reads and plans " +
			"only on the PostgreSQL wire; this server's CREATE TABLE takes its own table options and " +
			"none of them is the one the key names, so refusing it would answer a different question",
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
func roleManagement(t tableSpelling) experiment {
	const role = "ptah_capprobe_role"
	return experiment{
		decides: []capability.Capability{capability.RoleManagement},
		setup:   []string{t.table("rm_t", "n int", "n")},
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

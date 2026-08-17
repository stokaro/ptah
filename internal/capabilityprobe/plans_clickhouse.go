package capabilityprobe

import (
	"go.5x5.cz/ptah/core/platform/capability"
)

// clickHouseSpelling writes the throwaway tables the ClickHouse experiments
// need. ClickHouse has no default engine, so a CREATE TABLE without one is
// refused and every experiment would report the missing engine rather than the
// capability it asked about.
var clickHouseSpelling = tableSpelling{engine: "ENGINE=MergeTree ORDER BY %s"}

// clickHouseFunctionShapeStatement and clickHouseTTLShapeStatement are named
// rather than inlined because each is the statement a test has to be able to
// read back.
//
// Both keys have a ClickHouse statement that LOOKS like them and is accepted --
// the lambda alias `CREATE FUNCTION fn AS (x) -> x + 1`, and the MergeTree
// `TTL <expr>` clause -- and asking either would record support for an object
// the key does not name and no Ptah path can carry. Nothing offline can catch
// that substitution: both spellings compile, and only a live server tells them
// apart. So the shapes are pinned here and asserted (stokaro/ptah#916).
const clickHouseFunctionShapeStatement = "CREATE FUNCTION fn() RETURNS Int64 LANGUAGE SQL AS 'SELECT 1'"

func clickHouseTTLShapeStatement(t tableSpelling) string {
	return t.table("ttl_t", "n Int64, expires_at DateTime", "n") +
		" WITH (ttl_expiration_expression = 'expires_at')"
}

// clickHousePlan is the statement table for ClickHouse.
//
// It exists because the dialect had none, and a dialect with no plan is not a
// dialect that agrees with its preset: every row was undecidable with that
// stated, so four declared ClickHouse lines were carried by a preset nothing
// executed. stokaro/ptah#916 asks for a version ladder here, and a ladder needs
// something to measure the arms with.
//
// Written against a live 26.7.3.19 server, and it disagreed with the shipped
// preset on seven keys -- every one in the direction of ClickHouse doing MORE
// than Ptah claimed, so nothing was emitting DDL the server refuses; the cost
// was capability, not correctness.
//
// Two shapes the PostgreSQL plan needs are deliberately not used here.
//
// CONCURRENTLY is decided by plain acceptance rather than by the
// transaction-block discriminator: that discriminator exists because CockroachDB
// parses the keyword as a compatibility no-op, and ClickHouse answers
// `Syntax error` instead, which is not a thing a no-op does.
//
// The foreign-key experiment stays the enforcement one, and here it earns its
// keep twice over: ClickHouse ACCEPTS `FOREIGN KEY ... REFERENCES` in a CREATE
// TABLE and enforces nothing, so an acceptance probe would have recorded
// support for a constraint the engine ignores.
func clickHousePlan() plan {
	t := clickHouseSpelling
	experiments := []experiment{
		acceptance(capability.DropConstraintGeneric,
			[]string{t.table("dcg", "n Int64, CONSTRAINT dcg_ck CHECK n > 0", "n")},
			"ALTER TABLE dcg DROP CONSTRAINT dcg_ck",
		),
		{
			decides:  []capability.Capability{capability.DropConstraintIfExists},
			requires: []capability.Capability{capability.DropConstraintGeneric},
			setup:    []string{t.table("dcie", "n Int64", "n")},
			decide: guarded(capability.DropConstraintIfExists, nil,
				[]string{"ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent"},
				"ALTER TABLE dcie DROP CONSTRAINT dcie_absent",
			).decide,
		},
		guarded(capability.DropIndexIfExists,
			[]string{t.table("dii", "n Int64, s String, INDEX dii_idx s TYPE bloom_filter GRANULARITY 1", "n")},
			[]string{"ALTER TABLE dii DROP INDEX IF EXISTS dii_absent"},
			"ALTER TABLE dii DROP INDEX dii_absent",
		),
		enforced(capability.CheckConstraintsEnforced,
			[]string{t.table("cce", "n Int64, CONSTRAINT cce_ck CHECK n > 0", "n")},
			"INSERT INTO cce VALUES (1)",
			"INSERT INTO cce VALUES (-1)",
		),
		acceptance(capability.DropCheckClause,
			[]string{t.table("dcc", "n Int64, CONSTRAINT dcc_ck CHECK n > 0", "n")},
			"ALTER TABLE dcc DROP CHECK dcc_ck",
		),
		acceptance(capability.EnumInlineColumn, nil,
			t.table("eic", "c Enum8('a' = 1, 'b' = 2)", "c"),
		),
		acceptance(capability.EnumCustomType, nil,
			"CREATE TYPE ect AS ENUM ('a', 'b')",
		),
		acceptanceNote(capability.CreateIndexConcurrently,
			[]string{t.table("cic", "n Int64, s String", "n")},
			"CREATE INDEX CONCURRENTLY cic_one ON cic (s) TYPE bloom_filter GRANULARITY 1",
			"acceptance decides it here: ClickHouse answers Syntax error rather than "+
				"parsing CONCURRENTLY as a no-op, which is what the transaction-block "+
				"discriminator exists to catch elsewhere",
		),
		acceptance(capability.DropIndexConcurrently,
			[]string{t.table("dic", "n Int64, s String, INDEX dic_idx s TYPE bloom_filter GRANULARITY 1", "n")},
			"ALTER TABLE dic DROP INDEX CONCURRENTLY dic_idx",
		),
		acceptance(capability.IndexIncludeSPGiST,
			[]string{t.table("iis", "k String, payload Int64", "k")},
			"CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload)",
		),
		acceptance(capability.Views,
			[]string{t.table("vsrc", "n Int64", "n")},
			"CREATE VIEW vw AS SELECT n FROM vsrc",
		),
		acceptance(capability.MaterializedViews,
			[]string{t.table("mvs", "n Int64", "n")},
			"CREATE MATERIALIZED VIEW mvw ENGINE=MergeTree ORDER BY n AS SELECT n FROM mvs",
		),
		// The shape ast.CreateFunctionNode describes -- a return type, a
		// language and a body -- and NOT ClickHouse's lambda alias
		// `CREATE FUNCTION fn AS (x) -> x + 1`, which the server accepts and
		// the node cannot carry. Asking the accepted statement would have
		// recorded support for an object no Ptah path can emit or read.
		acceptanceNote(capability.Functions, nil,
			clickHouseFunctionShapeStatement,
			"the key names the standalone function object with a return type, a "+
				"language and a body; ClickHouse's lambda alias is a different object "+
				"and is accepted",
		),
		acceptance(capability.Triggers,
			[]string{t.table("trg_t", "n Int64", "n")},
			"CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_fn()",
		),
		{
			decides:  []capability.Capability{capability.CreateOrReplaceTrigger},
			requires: []capability.Capability{capability.Triggers},
			setup:    []string{t.table("cort_t", "n Int64", "n")},
			decide: acceptance(capability.CreateOrReplaceTrigger, nil,
				"CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW EXECUTE FUNCTION cort_fn()",
			).decide,
		},
		// The SQL-standard spelling, which ClickHouse does not parse. It has
		// generated columns as `MATERIALIZED <expr>` -- the very setup the next
		// experiment uses -- so this pair of answers is the reason the registry
		// carries no implication edge between the two keys.
		acceptance(capability.GeneratedColumns, nil,
			t.table("gcx", "n Int64, g Int64 GENERATED ALWAYS AS (n + 1) STORED", "n"),
		),
		acceptance(capability.AlterGeneratedColumnExpression,
			[]string{t.table("agc", "n Int64, g Int64 MATERIALIZED n + 1", "n")},
			"ALTER TABLE agc MODIFY COLUMN g Int64 MATERIALIZED n + 2",
		),
		acceptance(capability.RowLevelSecurity,
			[]string{t.table("rls", "n Int64", "n")},
			"ALTER TABLE rls ENABLE ROW LEVEL SECURITY",
		),
		acceptanceNote(capability.PostgresCatalogFunctions, nil,
			"SELECT obj_description(2200, 'pg_namespace')",
			"pg_catalog's introspection helpers; ClickHouse answers "+
				"`Unknown function obj_description`",
		),
		acceptanceNote(capability.CatalogRowStatistics, nil,
			"SELECT 1 FROM pg_stat_all_tables LIMIT 1",
			"the planner statistics view; ClickHouse answers "+
				"`Unknown table expression identifier`",
		),
		acceptanceNote(capability.CatalogDependencies, nil,
			"SELECT 1 FROM pg_depend LIMIT 1",
			"the dependency table the user-defined-type read joins; ClickHouse "+
				"answers `Unknown table expression identifier`",
		),
		roleManagement(t),
		foreignKeys(
			[]string{
				t.table("fk_parent", "id Int64", "id"),
				t.table("fk_child", "id Int64", "id"),
			},
			t.table("fk_child_fk", "id Int64, FOREIGN KEY (id) REFERENCES fk_parent (id)", "id"),
			"INSERT INTO fk_parent VALUES (1)",
			"INSERT INTO fk_child_fk VALUES (1)",
			"INSERT INTO fk_child_fk VALUES (99)",
		),
		referencePolicy(referencePolicyStatements{
			setup: []string{
				t.table("fkp_uni", "k Int64", "k"),
				t.table("fkp_idx", "k Int64", "k"),
				t.table("fkp_none", "k Int64", "k"),
				t.table("fkp_c0", "k Int64", "k"),
				t.table("fkp_c1", "k Int64", "k"),
				t.table("fkp_c2", "k Int64", "k"),
			},
			unique:  "ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k)",
			indexed: "ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k)",
			bare:    "ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k)",
		}),
		all(capability.Sequences, nil,
			"CREATE SEQUENCE sq",
		),
		acceptance(capability.XMLType, nil,
			t.table("xmlt", "c XML", "c"),
		),
		all(capability.AdvisoryLocks, nil,
			"SELECT pg_advisory_lock(1)",
			"SELECT pg_advisory_unlock(1)",
		),
		// The storage-parameter shape, and NOT the MergeTree `TTL <expr>`
		// clause the server accepts. This key names a policy declared as
		// storage parameters, which is the form CockroachDB answers and the
		// PostgreSQL-family probe reads back out of pg_class.reloptions.
		acceptanceNote(capability.RowLevelTTL, nil,
			clickHouseTTLShapeStatement(t),
			"the key names a row-expiry policy declared as storage parameters; "+
				"ClickHouse's MergeTree TTL clause is a different declaration and is "+
				"accepted",
		),
		// The key ClickHouse's own ladder turns on, asked of the server rather
		// than compared against 24.11. `CHECK GRANT` is a ClickHouse statement
		// with no counterpart elsewhere, so this is the one plan where it is a
		// real question rather than a refusal (stokaro/ptah#916).
		acceptanceNote(capability.CheckGrantStatement, nil,
			"CHECK GRANT SHOW DATABASES, SHOW TABLES ON *.*",
			"the statement that answers whether this account may see a catalog entry",
		),
		acceptanceNote(capability.CatalogViewDependencies, nil,
			"SELECT 1 FROM information_schema.view_table_usage LIMIT 1",
			"the catalog naming the tables a view reads",
		),
		// ClickHouse parses no FOREIGN KEY clause at all, so the whole statement
		// is refused rather than just the deferral (stokaro/ptah#1624).
		acceptance(capability.DeferrableConstraints,
			append(t.uniquelyReferenced("dfp", "dfp_uq", "id"), t.table("dfc", "n Int64, id Int64", "n")),
			"ALTER TABLE dfc ADD CONSTRAINT dfc_fk FOREIGN KEY (id) REFERENCES dfp (id) DEFERRABLE INITIALLY DEFERRED",
		),
		acceptanceNote(capability.CatalogCheckConstraintTableName, nil,
			"SELECT table_name FROM information_schema.check_constraints LIMIT 1",
			"the CHECK_CONSTRAINTS column that names the declaring table",
		),
		// The renamed column is deliberately NOT the sorting key: renaming that
		// one is refused for a reason that has nothing to do with the clause,
		// and asking it there would answer a different question.
		acceptance(capability.RenameColumnClause,
			[]string{t.table("rnc_t", "n Int64, b Int64", "n")},
			"ALTER TABLE rnc_t RENAME COLUMN b TO c",
		),
	}
	return plan{experiments: experiments, undecided: map[capability.Capability]string{
		// See the same declaration in the PostgreSQL-family plan: the probe
		// connects as one account and cannot ask whether a privilege exists
		// without granting it. ClickHouse has no SHOW_ROUTINE at all, which is
		// a fact the preset carries rather than one this run can establish.
		capability.ShowRoutinePrivilege: "the probe cannot ask whether a privilege exists without granting it; " +
			"ClickHouse has no SHOW_ROUTINE privilege for it to find",
	}}
}

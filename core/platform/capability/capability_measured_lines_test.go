package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// answer is what one live server did about one capability, with the statement
// that decided it and the server's verdict.
//
// The from field is not decoration. A capability claim is a claim about what a
// database does, so a row without a statement behind it is a claim nobody
// executed; the test below refuses an empty one. Where several statements ran,
// the one recorded here is the one whose verdict decided the key — the others
// are setup, and the preset doc comments name them.
type answer struct {
	does bool
	from string
}

// measuredLine is a release line whose capability answers were read off a live
// server rather than inherited from the line below it.
//
// PostgreSQL 18, MySQL 26.7 and MariaDB 12.3 are those lines. Each of them
// resolves onto the preset of the line beneath it — Postgres17, MySQL84 and
// MariaDB1011 — and that equality is this table's subject: it is what the
// servers answered, not an assumption. The only way to keep it honest is to
// hold the resolved set against the observations rather than against the
// preset it resembles.
type measuredLine struct {
	// dialect is what a caller passes to ResolveServerVersion beside the
	// banner, so the row resolves the way a live connection does.
	dialect string
	// banner is what the server reported for SELECT version() / VERSION().
	banner string
	// run and artifact name the capability-matrix run and the per-cell upload
	// within it, so a reader can fetch the full transcript rather than trusting
	// this transcription. internal/capabilityprobe/cells.go names the same run
	// beside each of these three lines.
	run      string
	artifact string
	// control is the statement the server had to refuse for any acceptance in
	// the run to be worth reading.
	control string
	// observed is one entry per registry key the probe decided from a
	// statement the server accepted or refused.
	observed map[capability.Capability]answer
	// carried names the keys no statement decided, with the probe plan's own
	// reason for declining to ask. Their values come from the preset beneath
	// and are NOT a measurement of this line.
	carried map[capability.Capability]string
}

// TestMeasuredLines_ResolveToTheRowsTheServersAnswered holds each measured
// release line against the server it was measured on, key by key, along the
// path an operator actually gets: the banner the server reports goes into
// ResolveServerVersion, and the capability set that call returns is what every
// row below is checked against. There is no per-line preset function to check
// instead — these three lines resolve onto the preset of the line beneath them
// — so the resolver is both the subject and the only honest source of the set,
// and the assertion covers the whole chain from reported version to resolved
// capabilities rather than a preset a caller would never reach.
//
// Without this table, nothing distinguishes "measured and found identical to
// the line below" from "copied and adjusted by reasoning". Every existing
// resolver test compares the resolved set against that lower line's preset,
// and such a comparison passes either way. The observations are the only thing
// that can tell the two apart, so they live here in full.
//
// The coverage assertion is the other half. Every registered key must appear
// in exactly one of the two maps, so a row cannot be dropped from the
// transcription and leave a resolved value nothing checks — the failure mode a
// hand-maintained subset has — and a key added to the registry later cannot
// arrive already presented as measured on these three servers.
func TestMeasuredLines_ResolveToTheRowsTheServersAnswered(t *testing.T) {
	for name, line := range measuredLines() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			resolved := capability.ResolveServerVersion(line.dialect, line.banner).Capabilities
			c.Assert(resolved.Validate(), qt.IsNil)
			c.Assert(line.banner, qt.Not(qt.Equals), "")
			c.Assert(line.run, qt.Not(qt.Equals), "")
			c.Assert(line.artifact, qt.Not(qt.Equals), "")
			c.Assert(line.control, qt.Not(qt.Equals), "")

			c.Assert(len(line.observed)+len(line.carried), qt.Equals, len(capability.All()),
				qt.Commentf("the transcription must account for every registered key exactly once"))
			for _, key := range capability.All() {
				_, wasObserved := line.observed[key]
				_, wasCarried := line.carried[key]
				c.Assert(wasObserved, qt.Not(qt.Equals), wasCarried,
					qt.Commentf("key %q is in neither map or in both; every key is measured or explicitly carried", key))
			}

			for key, observed := range line.observed {
				comment := qt.Commentf("%s on %s (artifact %s of run %s): %s",
					key, line.banner, line.artifact, line.run, observed.from)
				c.Check(observed.from, qt.Not(qt.Equals), "", comment)
				c.Check(resolved.Has(key), qt.Equals, observed.does, comment)
			}
			for key, reason := range line.carried {
				c.Check(reason, qt.Not(qt.Equals), "",
					qt.Commentf("key %q is carried rather than measured and must say why", key))
			}
		})
	}
}

// TestMeasuredLines_AreResolvedAsMeasuredLines ties the transcription to the
// resolver's own report about it: the banner is answered as a recognized
// release line (VersionSpecific) and not by running off the top of a ladder
// (Saturated). Without it the rows above would still pass for a banner the
// resolver had never heard of, because every ladder's open-topped arm hands
// back exactly the preset these three lines were found equal to — so a table
// of real observations could sit beside a resolver that had quietly stopped
// recognizing the line that produced them.
//
// What each half proves differs by dialect, and saying so is the point. The
// MySQL and MariaDB ladders match an exact major/minor line, so
// VersionSpecific here means "26.7 and 12.3 are named as measured". The
// PostgreSQL ladder is major-grained, so its VersionSpecific means only "at or
// below the newest measured major"; the neighbouring-line rows that separate
// each ladder from its siblings live in TestResolveServerVersionReportsSaturation
// and are not repeated here.
func TestMeasuredLines_AreResolvedAsMeasuredLines(t *testing.T) {
	for name, line := range measuredLines() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			resolution := capability.ResolveServerVersion(line.dialect, line.banner)

			comment := qt.Commentf("dialect=%s banner=%q", line.dialect, line.banner)
			c.Assert(resolution.VersionSpecific, qt.IsTrue, comment)
			c.Assert(resolution.Saturated, qt.IsFalse, comment)
		})
	}
}

// measuredLines transcribes the per-cell artifacts of the capability probe run
// that measured the three lines (issues #1339 and #1341). Every from string is
// a statement that run executed and the verdict the server returned; none of
// them is a reading of Ptah's own source.
func measuredLines() map[string]measuredLine {
	const probeRun = "31615442780"

	return map[string]measuredLine{
		"PostgreSQL 18": {
			dialect:  "postgres",
			banner:   "PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit",
			run:      probeRun,
			artifact: "capability-cell-postgres-18",
			control:  `REFUSED CREATE NONSENSE ptah_capability_probe_control -> ERROR: syntax error at or near "NONSENSE" (SQLSTATE 42601)`,
			observed: map[capability.Capability]answer{
				capability.AdvisoryLocks:                      {true, "ACCEPTED SELECT pg_advisory_unlock(1)"},
				capability.AlterGeneratedColumnExpression:     {true, "ACCEPTED ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2)"},
				capability.CheckConstraintsEnforced:           {true, "REFUSED INSERT INTO cce (n) VALUES (-1) -> violates check constraint \"cce_ck\" (SQLSTATE 23514)"},
				capability.CreateIndexConcurrently:            {true, "ACCEPTED CREATE INDEX CONCURRENTLY cic_one ON cic (n), then REFUSED inside a transaction block (SQLSTATE 25001)"},
				capability.CreateOrReplaceTrigger:             {true, "ACCEPTED CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW EXECUTE FUNCTION cort_fn()"},
				capability.DropCheckClause:                    {false, "REFUSED ALTER TABLE dcc DROP CHECK dcc_ck -> syntax error at or near \"CHECK\" (SQLSTATE 42601)"},
				capability.DropConstraintGeneric:              {true, "ACCEPTED ALTER TABLE dcg DROP CONSTRAINT dcg_uq"},
				capability.DropConstraintIfExists:             {true, "ACCEPTED ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent, while the unguarded drop reached SQLSTATE 42704"},
				capability.DropIndexConcurrently:              {true, "ACCEPTED DROP INDEX CONCURRENTLY dic_one, then REFUSED inside a transaction block (SQLSTATE 25001)"},
				capability.DropIndexIfExists:                  {true, "ACCEPTED DROP INDEX IF EXISTS dii_absent, while the unguarded drop reached SQLSTATE 42704"},
				capability.EnumCustomType:                     {true, "ACCEPTED CREATE TYPE ect AS ENUM ('a','b')"},
				capability.EnumInlineColumn:                   {false, "REFUSED CREATE TABLE eic (c ENUM('a','b')) -> type \"enum\" does not exist (SQLSTATE 42704)"},
				capability.ForeignKeys:                        {true, "REFUSED INSERT INTO fk_child (id) VALUES (99) -> violates foreign key constraint (SQLSTATE 23503), with the valid child row ACCEPTED"},
				capability.ForeignKeysCreateBackingIndex:      {false, "REFUSED ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k) -> SQLSTATE 42830"},
				capability.ForeignKeysRequireIndexedReference: {false, "REFUSED ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k) -> SQLSTATE 42830"},
				capability.ForeignKeysRequireUniqueReference:  {true, "ACCEPTED ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k) while both nonunique forms were REFUSED"},
				capability.Functions:                          {true, "ACCEPTED CREATE FUNCTION fn() RETURNS int LANGUAGE sql AS 'SELECT 1'"},
				capability.IndexIncludeSPGiST:                 {true, "ACCEPTED CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload), and pg_index reported indnkeyatts 1 with indnatts 2"},
				capability.MaterializedViews:                  {true, "ACCEPTED CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs, and SELECT c FROM mvw was unchanged by an INSERT into mvs"},
				capability.RoleManagement:                     {true, "ACCEPTED GRANT SELECT ON rm_t TO ptah_capprobe_role after ACCEPTED CREATE ROLE ptah_capprobe_role"},
				capability.RowLevelSecurity:                   {true, "ACCEPTED CREATE POLICY rls_p ON rls USING (true) after ACCEPTED ALTER TABLE rls ENABLE ROW LEVEL SECURITY"},
				capability.Sequences:                          {true, "ACCEPTED CREATE SEQUENCE sq and CREATE TABLE ser (id SERIAL PRIMARY KEY)"},
				capability.Triggers:                           {true, "ACCEPTED CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_fn()"},
				capability.Views:                              {true, "ACCEPTED CREATE VIEW vw AS SELECT n FROM vsrc"},
				capability.XMLType:                            {true, "ACCEPTED CREATE TABLE xmlt (c XML)"},
			},
			carried: map[capability.Capability]string{
				capability.PostgresCatalogFunctions: "the key exists for the Spanner PostgreSQL interface, whose catalog refuses obj_description with `The Postgres Type is not supported: name`; this run did not probe it, and a PostgreSQL server answering the function would not tell this line apart from the preset below",
				capability.CatalogRowStatistics:     "the key exists for the Spanner PostgreSQL interface, whose catalog carries the pg_class columns beside it and not pg_stat_all_tables; this run did not probe the statistics views, and a PostgreSQL server having them would not tell this line apart from the preset below",
				capability.CatalogDependencies:      "the key exists for the Spanner PostgreSQL interface, whose catalog has no pg_depend to join; this run did not create a domain, and a PostgreSQL server having them would not tell this line apart from the preset below",
				capability.RowLevelTTL: "this run predates the key and sent no TTL statement. PostgreSQL is the engine the key is false FOR, so a refusal here would restate the premise rather than measure this line; " +
					"what decides the key is CockroachDB accepting the parameter, which internal/capabilityprobe asks on the CockroachDB cells (stokaro/ptah#1027)",
				capability.CheckGrantStatement:             "the key names ClickHouse's CHECK GRANT, which this server has no spelling of; its answer would be to a different question. What decides the key is ClickHouse accepting the statement on one declared line and refusing it as a syntax error on another, which internal/capabilityprobe asks on the ClickHouse cells (stokaro/ptah#916)",
				capability.CatalogViewDependencies:         "the key names information_schema.VIEW_TABLE_USAGE, a MySQL catalog view. PostgreSQL answers view dependencies through pg_depend and pg_rewrite instead, which is a different catalog and a different question",
				capability.ShowRoutinePrivilege:            "the key names MySQL's global SHOW_ROUTINE privilege. PostgreSQL has no such privilege, so this server has nothing to demand or to do without",
				capability.RenameColumnClause:              "this run predates the key and sent no rename. Measured separately on PostgreSQL 18.4: `ALTER TABLE rn RENAME COLUMN a TO b` is accepted",
				capability.CatalogCheckConstraintTableName: "this run predates the key. Measured separately on PostgreSQL 18.4: `information_schema.check_constraints` is constraint_catalog, constraint_schema, constraint_name, check_clause -- no table_name",
				capability.GeneratedColumns:                "this run predates the key. Measured separately on PostgreSQL 18.4: `CREATE TABLE gcx (n int, g int GENERATED ALWAYS AS (n + 1) STORED)` is accepted",
			},
		},

		"MySQL 26": {
			dialect:  "mysql",
			banner:   "26.7.0",
			run:      probeRun,
			artifact: "capability-cell-mysql-26-7",
			control:  "REFUSED CREATE NONSENSE ptah_capability_probe_control -> Error 1064 (42000)",
			observed: map[capability.Capability]answer{
				capability.AdvisoryLocks:                      {false, "REFUSED SELECT pg_advisory_lock(1) -> Error 1305 (42000): FUNCTION pg_advisory_lock does not exist"},
				capability.AlterGeneratedColumnExpression:     {false, "REFUSED ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2) -> Error 1064 (42000)"},
				capability.CheckConstraintsEnforced:           {true, "REFUSED INSERT INTO cce (n) VALUES (-1) -> Error 3819 (HY000): Check constraint 'cce_ck' is violated"},
				capability.CreateIndexConcurrently:            {false, "REFUSED CREATE INDEX CONCURRENTLY cic_one ON cic (n) -> Error 1064 (42000)"},
				capability.CreateOrReplaceTrigger:             {false, "REFUSED CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW SET NEW.n = NEW.n -> Error 1064 (42000)"},
				capability.DropCheckClause:                    {true, "ACCEPTED ALTER TABLE dcc DROP CHECK dcc_ck"},
				capability.DropConstraintGeneric:              {true, "ACCEPTED ALTER TABLE dcg DROP CONSTRAINT dcg_uq"},
				capability.DropConstraintIfExists:             {false, "REFUSED ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent -> Error 1064 (42000), while the unguarded drop reached Error 3940 (HY000)"},
				capability.DropIndexConcurrently:              {false, "REFUSED DROP INDEX CONCURRENTLY dic_one ON dic -> Error 1064 (42000)"},
				capability.DropIndexIfExists:                  {false, "REFUSED DROP INDEX IF EXISTS dii_absent ON dii -> Error 1064 (42000), while the unguarded drop reached Error 1091 (42000)"},
				capability.EnumCustomType:                     {false, "REFUSED CREATE TYPE ect AS ENUM ('a','b') -> Error 1064 (42000)"},
				capability.EnumInlineColumn:                   {true, "ACCEPTED CREATE TABLE eic (c ENUM('a','b'))"},
				capability.ForeignKeys:                        {true, "REFUSED INSERT INTO fk_child (id) VALUES (99) -> Error 1452 (23000), with the valid child row ACCEPTED"},
				capability.ForeignKeysCreateBackingIndex:      {false, "REFUSED ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k) -> Error 6125 (HY000)"},
				capability.ForeignKeysRequireIndexedReference: {false, "REFUSED ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k) -> Error 6125 (HY000): Missing unique key"},
				capability.ForeignKeysRequireUniqueReference:  {true, "ACCEPTED ALTER TABLE fkp_c0 ADD CONSTRAINT fkp_s0 FOREIGN KEY (k) REFERENCES fkp_uni (k) while both nonunique forms were REFUSED"},
				capability.Functions:                          {true, "ACCEPTED CREATE FUNCTION fn() RETURNS INT DETERMINISTIC RETURN 1"},
				capability.IndexIncludeSPGiST:                 {false, "REFUSED CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload) -> Error 1064 (42000)"},
				capability.MaterializedViews:                  {false, "ACCEPTED CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs, but SELECT c FROM mvw reported 0 before an INSERT into mvs and 1 after, so the result is recomputed rather than stored"},
				capability.RowLevelSecurity:                   {false, "REFUSED ALTER TABLE rls ENABLE ROW LEVEL SECURITY -> Error 1064 (42000)"},
				capability.Sequences:                          {false, "REFUSED CREATE SEQUENCE sq -> Error 1064 (42000)"},
				capability.Triggers:                           {true, "ACCEPTED CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW SET NEW.n = NEW.n"},
				capability.Views:                              {true, "ACCEPTED CREATE VIEW vw AS SELECT n FROM vsrc"},
				capability.XMLType:                            {false, "REFUSED CREATE TABLE xmlt (c XML) -> Error 1064 (42000)"},
			},
			carried: map[capability.Capability]string{
				capability.PostgresCatalogFunctions: "obj_description is a PostgreSQL catalog function no MySQL-family code path consults; this server has no such function, so neither answering nor refusing it would decide the key",
				capability.CatalogRowStatistics:     "pg_stat_all_tables is a PostgreSQL statistics view no MySQL-family code path consults; this server has no such relation, so neither having nor lacking it would decide the key",
				capability.CatalogDependencies:      "domains are a PostgreSQL type-system feature no MySQL-family code path consults; this server has no CREATE DOMAIN at all, so neither accepting nor refusing one would decide the key",
				capability.RoleManagement: "the key names the role and privilege surface no MySQL-family code path consults; " +
					"this server's own CREATE ROLE and GRANT are a different surface, so accepting them would not decide the key",
				capability.RowLevelTTL: "the key names a table storage parameter no MySQL-family renderer, reader or planner emits; " +
					"this server has no such parameter to accept or refuse, so its answer would be to a different question",
				capability.CheckGrantStatement:             "the key names ClickHouse's CHECK GRANT, which this server has no spelling of; its answer would be to a different question. What decides the key is ClickHouse accepting the statement on one declared line and refusing it as a syntax error on another, which internal/capabilityprobe asks on the ClickHouse cells (stokaro/ptah#916)",
				capability.CatalogViewDependencies:         "this run predates the key and asked no catalog question for it. What decides it is whether information_schema.VIEW_TABLE_USAGE resolves, which MySQL added in 8.0.13 and every measured MySQL line has",
				capability.ShowRoutinePrivilege:            "this run predates the key. What decides it is whether the server grants SHOW_ROUTINE, which MySQL added in 8.0.20 and every measured MySQL line has",
				capability.RenameColumnClause:              "this run predates the key. Measured separately on MySQL 8.4.11: the clause is accepted",
				capability.CatalogCheckConstraintTableName: "this run predates the key. Measured separately on MySQL 8.4.11: `information_schema.CHECK_CONSTRAINTS` has no TABLE_NAME column; selecting it is error 1054",
				capability.GeneratedColumns:                "this run predates the key. Measured separately on MySQL 8.4.11: `CREATE TABLE gcx (n int, g int GENERATED ALWAYS AS (n + 1) STORED)` is accepted",
			},
		},

		"MariaDB 12": {
			dialect:  "mariadb",
			banner:   "12.3.2-MariaDB-ubu2404",
			run:      probeRun,
			artifact: "capability-cell-mariadb-12-3",
			control:  "REFUSED CREATE NONSENSE ptah_capability_probe_control -> Error 1064 (42000)",
			observed: map[capability.Capability]answer{
				capability.AdvisoryLocks:                      {false, "REFUSED SELECT pg_advisory_lock(1) -> Error 1305 (42000): FUNCTION pg_advisory_lock does not exist"},
				capability.AlterGeneratedColumnExpression:     {false, "REFUSED ALTER TABLE agc ALTER COLUMN g SET EXPRESSION AS (n + 2) -> Error 1064 (42000)"},
				capability.CheckConstraintsEnforced:           {true, "REFUSED INSERT INTO cce (n) VALUES (-1) -> Error 4025 (23000): CONSTRAINT `cce_ck` failed"},
				capability.CreateIndexConcurrently:            {false, "REFUSED CREATE INDEX CONCURRENTLY cic_one ON cic (n) -> Error 1064 (42000)"},
				capability.CreateOrReplaceTrigger:             {true, "ACCEPTED CREATE OR REPLACE TRIGGER cort BEFORE INSERT ON cort_t FOR EACH ROW SET NEW.n = NEW.n"},
				capability.DropCheckClause:                    {false, "REFUSED ALTER TABLE dcc DROP CHECK dcc_ck -> Error 1064 (42000)"},
				capability.DropConstraintGeneric:              {true, "ACCEPTED ALTER TABLE dcg DROP CONSTRAINT dcg_uq"},
				capability.DropConstraintIfExists:             {true, "ACCEPTED ALTER TABLE dcie DROP CONSTRAINT IF EXISTS dcie_absent and ALTER TABLE dcie DROP FOREIGN KEY IF EXISTS dcie_absent_fk, while the unguarded drop reached Error 1091 (42000)"},
				capability.DropIndexConcurrently:              {false, "REFUSED DROP INDEX CONCURRENTLY dic_one ON dic -> Error 1064 (42000)"},
				capability.DropIndexIfExists:                  {true, "ACCEPTED DROP INDEX IF EXISTS dii_absent ON dii, while the unguarded drop reached Error 1091 (42000)"},
				capability.EnumCustomType:                     {false, "REFUSED CREATE TYPE ect AS ENUM ('a','b') -> Error 1064 (42000)"},
				capability.EnumInlineColumn:                   {true, "ACCEPTED CREATE TABLE eic (c ENUM('a','b'))"},
				capability.ForeignKeys:                        {true, "REFUSED INSERT INTO fk_child (id) VALUES (99) -> Error 1452 (23000), with the valid child row ACCEPTED"},
				capability.ForeignKeysCreateBackingIndex:      {false, "REFUSED ALTER TABLE fkp_c2 ADD CONSTRAINT fkp_s2 FOREIGN KEY (k) REFERENCES fkp_none (k) -> Error 1005 (HY000) errno 150"},
				capability.ForeignKeysRequireIndexedReference: {true, "ACCEPTED ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k) while the bare-column form was REFUSED"},
				capability.ForeignKeysRequireUniqueReference:  {false, "ACCEPTED ALTER TABLE fkp_c1 ADD CONSTRAINT fkp_s1 FOREIGN KEY (k) REFERENCES fkp_idx (k), so a declared unique key is not required"},
				capability.Functions:                          {true, "ACCEPTED CREATE FUNCTION fn() RETURNS INT DETERMINISTIC RETURN 1"},
				capability.IndexIncludeSPGiST:                 {false, "REFUSED CREATE INDEX iis_idx ON iis USING SPGIST (k) INCLUDE (payload) -> Error 1064 (42000)"},
				capability.MaterializedViews:                  {false, "REFUSED CREATE MATERIALIZED VIEW mvw AS SELECT COUNT(*) AS c FROM mvs -> Error 1064 (42000)"},
				capability.RowLevelSecurity:                   {false, "REFUSED ALTER TABLE rls ENABLE ROW LEVEL SECURITY -> Error 1064 (42000)"},
				capability.Triggers:                           {true, "ACCEPTED CREATE TRIGGER trg BEFORE INSERT ON trg_t FOR EACH ROW SET NEW.n = NEW.n"},
				capability.Views:                              {true, "ACCEPTED CREATE VIEW vw AS SELECT n FROM vsrc"},
				capability.XMLType:                            {false, "REFUSED CREATE TABLE xmlt (c XML) -> Error 4161 (HY000): Unknown data type: 'XML'"},
			},
			carried: map[capability.Capability]string{
				capability.PostgresCatalogFunctions: "obj_description is a PostgreSQL catalog function no MySQL-family code path consults; this server has no such function, so neither answering nor refusing it would decide the key",
				capability.CatalogRowStatistics:     "pg_stat_all_tables is a PostgreSQL statistics view no MySQL-family code path consults; this server has no such relation, so neither having nor lacking it would decide the key",
				capability.CatalogDependencies:      "domains are a PostgreSQL type-system feature no MySQL-family code path consults; this server has no CREATE DOMAIN at all, so neither accepting nor refusing one would decide the key",
				capability.RoleManagement: "the key names the role and privilege surface no MySQL-family code path consults; " +
					"this server's own CREATE ROLE and GRANT are a different surface, so accepting them would not decide the key",
				capability.Sequences: "the key describes Ptah's generator rather than the engine (stokaro/ptah#931 item 8): " +
					"MariaDB has had SEQUENCE since 10.3 while no MySQL-family renderer or planner emits, reads or plans one, " +
					"so the server's answer is to a different question",
				capability.RowLevelTTL: "the key names a table storage parameter no MySQL-family renderer, reader or planner emits; " +
					"this server has no such parameter to accept or refuse, so its answer would be to a different question",
				capability.CheckGrantStatement:             "the key names ClickHouse's CHECK GRANT, which this server has no spelling of; its answer would be to a different question. What decides the key is ClickHouse accepting the statement on one declared line and refusing it as a syntax error on another, which internal/capabilityprobe asks on the ClickHouse cells (stokaro/ptah#916)",
				capability.CatalogViewDependencies:         "MariaDB has no information_schema.VIEW_TABLE_USAGE at any version, so the key is false for the dialect rather than for this line",
				capability.ShowRoutinePrivilege:            "MariaDB has no SHOW_ROUTINE privilege; its routine metadata is reached under the privileges the MariaDB branch of the visibility check already names",
				capability.RenameColumnClause:              "this run predates the key. Measured separately on MariaDB 11.8.8: the clause is accepted",
				capability.CatalogCheckConstraintTableName: "this run predates the key. Measured separately on MariaDB 11.8.8: `information_schema.CHECK_CONSTRAINTS` carries TABLE_NAME between CONSTRAINT_SCHEMA and CONSTRAINT_NAME",
				capability.GeneratedColumns:                "this run predates the key. Measured separately on MariaDB 11.8.8: `CREATE TABLE gcx (n int, g int GENERATED ALWAYS AS (n + 1) STORED)` is accepted",
			},
		},
	}
}

package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// answer is what one live server did about one capability, with the statement
// that decided it and the server's verdict.
//
// The evidence field is not decoration. A capability claim is a claim about
// what a database does, so a row without a statement behind it is a claim
// nobody executed; the test below refuses an empty one. Where several
// statements ran, the one recorded here is the one whose verdict decided the
// key — the others are setup, and the preset doc comments name them.
type answer struct {
	does bool
	from string
}

// measuredLine is a release line whose preset was built from a live server's
// answers rather than inherited from the line below it.
//
// The three lines below are PostgreSQL 18, MySQL 26 and MariaDB 12, added in
// the change that measured them. Each preset carries exactly the rows of the
// preset beneath it, and that equality is this table's subject: it is what the
// servers answered, not an assumption, and the only way to keep it honest is
// to hold the preset against the observations rather than against the preset
// it resembles.
type measuredLine struct {
	preset func() capability.Capabilities
	// banner is what the server reported for SELECT version() / VERSION().
	banner string
	// artifact names the probe run's per-cell upload, so a reader can fetch
	// the full transcript rather than trusting this transcription.
	artifact string
	// control is the statement the server had to refuse for any acceptance in
	// the run to be worth reading.
	control string
	// observed is one entry per registry key the probe decided from a
	// statement the server accepted or refused.
	observed map[capability.Capability]answer
	// carried names the keys no statement decided, with the probe plan's own
	// reason for declining to ask. Their values come from the preset below and
	// are NOT a measurement of this line.
	carried map[capability.Capability]string
}

// TestMeasuredLines_CarryTheRowsTheServersAnswered holds each newly measured
// preset against the server it was measured on, key by key.
//
// Without it, nothing distinguishes "measured and found identical" from
// "copied and adjusted by reasoning", because all three presets return the set
// of the line below them: a set comparison against that line passes either
// way, and so does every existing resolver test. The observations are the only
// thing that can tell the two apart, so they live here in full.
//
// The coverage assertion is the other half. Every registered key must appear
// in exactly one of the two maps, so a row cannot be dropped from the
// transcription and leave a preset value nothing checks — the failure mode a
// hand-maintained subset has.
func TestMeasuredLines_CarryTheRowsTheServersAnswered(t *testing.T) {
	for name, line := range measuredLines() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			preset := line.preset()
			c.Assert(preset.Validate(), qt.IsNil)
			c.Assert(line.banner, qt.Not(qt.Equals), "")
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
				comment := qt.Commentf("%s on %s: %s", key, line.banner, observed.from)
				c.Check(observed.from, qt.Not(qt.Equals), "", comment)
				c.Check(preset.Has(key), qt.Equals, observed.does, comment)
			}
			for key, reason := range line.carried {
				c.Check(reason, qt.Not(qt.Equals), "",
					qt.Commentf("key %q is carried rather than measured and must say why", key))
			}
		})
	}
}

// TestMeasuredLines_AreTheOnesTheResolverHandsOut ties the transcription to the
// resolver: the preset each line's banner resolves to is the preset the rows
// above were checked against, and the resolution is version-specific rather
// than saturated. Without this, the table could describe presets no live
// server ever receives.
func TestMeasuredLines_AreTheOnesTheResolverHandsOut(t *testing.T) {
	dialects := map[string]string{
		"PostgreSQL 18": "postgres",
		"MySQL 26":      "mysql",
		"MariaDB 12":    "mariadb",
	}
	for name, line := range measuredLines() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			resolution := capability.ResolveServerVersion(dialects[name], line.banner)

			comment := qt.Commentf("banner %q", line.banner)
			c.Assert(resolution.Capabilities, qt.DeepEquals, line.preset(), comment)
			c.Assert(resolution.VersionSpecific, qt.IsTrue, comment)
			c.Assert(resolution.Saturated, qt.IsFalse, comment)
		})
	}
}

// measuredLines transcribes the per-cell artifacts of the capability probe run
// that measured the three lines (issues #1339 and #1341). Every `from` string
// is a statement that run executed and the verdict the server returned.
func measuredLines() map[string]measuredLine {
	return map[string]measuredLine{
		"PostgreSQL 18": {
			preset:   capability.Postgres18,
			banner:   "PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit",
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
			carried: map[capability.Capability]string{},
		},

		"MySQL 26": {
			preset:   capability.MySQL26,
			banner:   "26.7.0",
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
				capability.RoleManagement: "the key names the PostgreSQL role and privilege surface no MySQL-family code path consults; " +
					"this server's own CREATE ROLE and GRANT are a different surface, so accepting them would not decide the key",
			},
		},

		"MariaDB 12": {
			preset:   capability.MariaDB12,
			banner:   "12.3.2-MariaDB-ubu2404",
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
				capability.RoleManagement: "the key names the PostgreSQL role and privilege surface no MySQL-family code path consults; " +
					"this server's own CREATE ROLE and GRANT are a different surface, so accepting them would not decide the key",
				capability.Sequences: "the key describes Ptah's generator rather than the engine (stokaro/ptah#931 item 8): " +
					"MariaDB has had SEQUENCE since 10.3 while no MySQL-family renderer or planner emits, reads or plans one, " +
					"so the server's answer is to a different question",
			},
		},
	}
}

package capabilityprobe

import (
	"context"
	"maps"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// probeComment is the text every object-comment experiment writes and reads
// back.
const probeComment = "ptah capability probe"

// withObjectComments answers the object-comment keys in a dialect's plan.
// They are added where the plan is assembled rather than inside each family's
// plan, because every dialect answers them the same way for its kind: the
// PostgreSQL family runs the experiments, a dialect with no COMMENT ON at all
// records the refusal, and one where no statement could decide them says why.
func withObjectComments(p plan, dialect string) plan {
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		t := postgresFamilySpelling(dialect)
		p.experiments = append(p.experiments, objectCommentExperiments(t)...)
		p.experiments = append(p.experiments, routineCommentExperiments(t)...)
	case platform.SQLServer:
		p.experiments = append(p.experiments, objectCommentRefusals("T-SQL has no COMMENT ON statement at all; "+
			"SQL Server carries a comment as an extended property, which this key does not name")...)
	case platform.SQLite:
		p.experiments = append(p.experiments, objectCommentRefusals("SQLite has no COMMENT ON statement at all")...)
	case platform.MySQL, platform.MariaDB:
		p.undecided = withObjectCommentsUndecided(p.undecided, "MySQL and MariaDB have no COMMENT ON statement; "+
			"a comment lives in a table or column clause, and neither renderer writes one for these "+
			"objects, so there is no statement whose answer would decide the key")
	case platform.ClickHouse:
		p.undecided = withObjectCommentsUndecided(p.undecided, "ClickHouse comments a view in its CREATE and "+
			"has no COMMENT ON statement for these objects, and its renderer writes none, so there is no "+
			"statement whose answer would decide the key")
	case platform.Oracle:
		p.undecided = withObjectCommentsUndecided(p.undecided, "the key names a PostgreSQL-family COMMENT ON "+
			"form that Ptah writes only through the PostgreSQL renderer; the Oracle renderer writes no comment "+
			"for these objects, so an answer here would be to a different question")
	}
	return p
}

// objectCommentExperiments asks the PostgreSQL family the object-comment
// questions of views, sequences, types, domains and extensions, one experiment
// per statement, because the engines take different subsets of them
// (stokaro/ptah#3627).
//
// Each object is created by the experiment inside the probe's own schema, so
// dropping that schema at the end of the run removes it. The extension is the
// one that needs a word: an unqualified CREATE EXTENSION installs into the
// first schema on the search path, which is the probe's, and DROP SCHEMA ...
// CASCADE drops an extension installed there. fuzzystrmatch is used because
// PostgreSQL's contrib and YugabyteDB both ship it, CockroachDB accepts the
// CREATE, and nothing else in this repository installs it -- a server that
// already carries it refuses the CREATE and reads as lacking the key.
func objectCommentExperiments(t tableSpelling) []experiment {
	return []experiment{
		objectComment(capability.ViewComments,
			[]capability.Capability{capability.Views},
			[]string{t.table("vcm_t", "n int", "n"), "CREATE VIEW vcm AS SELECT n FROM vcm_t"},
			[]string{"COMMENT ON VIEW vcm IS '" + probeComment + "'"},
			"SELECT COUNT(*) WHERE obj_description('vcm'::regclass, 'pg_class') = '"+probeComment+"'",
		),
		objectComment(capability.SequenceComments,
			[]capability.Capability{capability.Sequences},
			[]string{"CREATE SEQUENCE scm"},
			[]string{"COMMENT ON SEQUENCE scm IS '" + probeComment + "'"},
			"SELECT COUNT(*) WHERE obj_description('scm'::regclass, 'pg_class') = '"+probeComment+"'",
		),
		// A composite rather than a range, because it is the type CockroachDB
		// creates: its answer is the one that separates acceptance from a
		// comment that can be read back.
		objectComment(capability.TypeComments,
			[]capability.Capability{capability.CompositeTypes},
			[]string{"CREATE TYPE tcm AS (n int)"},
			[]string{"COMMENT ON TYPE tcm IS '" + probeComment + "'"},
			"SELECT COUNT(*) WHERE obj_description('tcm'::regtype, 'pg_type') = '"+probeComment+"'",
		),
		objectComment(capability.DomainComments,
			[]capability.Capability{capability.DomainTypes},
			[]string{"CREATE DOMAIN dcm AS int"},
			[]string{"COMMENT ON DOMAIN dcm IS '" + probeComment + "'"},
			"SELECT COUNT(*) WHERE obj_description('dcm'::regtype, 'pg_type') = '"+probeComment+"'",
		),
		// The CREATE is part of the question rather than its setup. The
		// Spanner interface refuses it with `Statement is not supported`, and
		// a target that cannot create an extension has none to hold a comment.
		objectComment(capability.ExtensionComments, nil, nil,
			[]string{
				"CREATE EXTENSION fuzzystrmatch",
				"COMMENT ON EXTENSION fuzzystrmatch IS '" + probeComment + "'",
			},
			"SELECT COUNT(*) FROM pg_extension WHERE extname = 'fuzzystrmatch' "+
				"AND obj_description(oid, 'pg_extension') = '"+probeComment+"'",
		),
	}
}

// routineCommentExperiments asks the PostgreSQL family the comment questions of
// functions, procedures, materialized views, triggers and policies
// (stokaro/ptah#3646). Each read-back addresses the object the way Ptah's
// reader does: a routine through pg_proc in the probe's schema, a trigger and
// a policy through the table they belong to.
func routineCommentExperiments(t tableSpelling) []experiment {
	const inProbeSchema = "JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = current_schema() "
	return []experiment{
		objectComment(capability.FunctionComments,
			[]capability.Capability{capability.Functions},
			[]string{"CREATE FUNCTION fncm(a int) RETURNS int LANGUAGE sql AS 'SELECT 1'"},
			[]string{"COMMENT ON FUNCTION fncm(int) IS '" + probeComment + "'"},
			"SELECT COUNT(*) FROM pg_proc p "+inProbeSchema+
				"AND p.proname = 'fncm' AND obj_description(p.oid, 'pg_proc') = '"+probeComment+"'",
		),
		objectComment(capability.ProcedureComments,
			[]capability.Capability{capability.Procedures},
			[]string{"CREATE PROCEDURE prcm(a int) LANGUAGE sql AS 'SELECT 1'"},
			[]string{"COMMENT ON PROCEDURE prcm(int) IS '" + probeComment + "'"},
			"SELECT COUNT(*) FROM pg_proc p "+inProbeSchema+
				"AND p.proname = 'prcm' AND obj_description(p.oid, 'pg_proc') = '"+probeComment+"'",
		),
		objectComment(capability.MaterializedViewComments,
			[]capability.Capability{capability.MaterializedViews},
			[]string{t.table("mvcm_t", "n int", "n"), "CREATE MATERIALIZED VIEW mvcm AS SELECT n FROM mvcm_t"},
			[]string{"COMMENT ON MATERIALIZED VIEW mvcm IS '" + probeComment + "'"},
			"SELECT COUNT(*) WHERE obj_description('mvcm'::regclass, 'pg_class') = '"+probeComment+"'",
		),
		objectComment(capability.TriggerComments,
			[]capability.Capability{capability.Triggers},
			[]string{
				t.table("tgcm_t", "n int", "n"),
				"CREATE FUNCTION tgcm_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
				"CREATE TRIGGER tgcm BEFORE INSERT ON tgcm_t FOR EACH ROW EXECUTE FUNCTION tgcm_fn()",
			},
			[]string{"COMMENT ON TRIGGER tgcm ON tgcm_t IS '" + probeComment + "'"},
			"SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = 'tgcm_t'::regclass AND tgname = 'tgcm' "+
				"AND obj_description(oid, 'pg_trigger') = '"+probeComment+"'",
		),
		objectComment(capability.PolicyComments,
			[]capability.Capability{capability.RowLevelSecurity},
			[]string{
				t.table("plcm_t", "n int", "n"),
				"ALTER TABLE plcm_t ENABLE ROW LEVEL SECURITY",
				"CREATE POLICY plcm ON plcm_t USING (true)",
			},
			[]string{"COMMENT ON POLICY plcm ON plcm_t IS '" + probeComment + "'"},
			"SELECT COUNT(*) FROM pg_policy WHERE polrelid = 'plcm_t'::regclass AND polname = 'plcm' "+
				"AND obj_description(oid, 'pg_policy') = '"+probeComment+"'",
		),
	}
}

// objectComment decides one object-comment key by writing a comment and
// reading it back through obj_description, the function Ptah's reader uses.
//
// Acceptance alone would not decide it. CockroachDB accepts COMMENT ON TYPE
// and obj_description then reports NULL, so a key decided by acceptance would
// have Ptah write a comment it can never read back, and plan it again on every
// run. readBack is a COUNT that answers 1 when the comment is where the reader
// looks.
func objectComment(
	key capability.Capability,
	requires []capability.Capability,
	setup, statements []string,
	readBack string,
) experiment {
	return experiment{
		decides:  []capability.Capability{key},
		requires: requires,
		setup:    setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempts, accepted := s.runAll(ctx, statements)
			if !accepted {
				return verdicts{key: decided(false)}, attempts
			}
			stored, read := s.query(ctx, readBack)
			return verdicts{key: objectCommentObservation(read, stored)}, append(attempts, read)
		},
	}
}

// objectCommentObservation reads the read-back that follows an accepted
// comment: one row is a comment where the reader looks, and none is a comment
// the server took and does not report.
func objectCommentObservation(read Attempt, stored int64) observation {
	if !read.Accepted {
		return cannotDecide(
			"the comment was accepted and the read-back %q was refused (%s)",
			collapse(read.Statement), collapse(read.ServerErr),
		)
	}
	if stored != 1 {
		return annotated(false, "the server accepted the comment and obj_description does not report it")
	}
	return decided(true)
}

// objectCommentStatements is the COMMENT ON form each object-comment key
// names, for a dialect asked to refuse it.
var objectCommentStatements = []struct {
	key       capability.Capability
	statement string
}{
	{capability.ViewComments, "COMMENT ON VIEW vcm IS 'probe'"},
	{capability.SequenceComments, "COMMENT ON SEQUENCE scm IS 'probe'"},
	{capability.TypeComments, "COMMENT ON TYPE tcm IS 'probe'"},
	{capability.DomainComments, "COMMENT ON DOMAIN dcm IS 'probe'"},
	{capability.ExtensionComments, "COMMENT ON EXTENSION ecm IS 'probe'"},
	{capability.FunctionComments, "COMMENT ON FUNCTION fncm(int) IS 'probe'"},
	{capability.ProcedureComments, "COMMENT ON PROCEDURE prcm(int) IS 'probe'"},
	{capability.MaterializedViewComments, "COMMENT ON MATERIALIZED VIEW mvcm IS 'probe'"},
	{capability.TriggerComments, "COMMENT ON TRIGGER tgcm ON tgcm_t IS 'probe'"},
	{capability.PolicyComments, "COMMENT ON POLICY plcm ON plcm_t IS 'probe'"},
}

// objectCommentRefusals asks a dialect with no COMMENT ON statement at all
// each object-comment question, so its answers are refusals the run recorded
// rather than values a preset asserted. The statement names no object that
// exists because the grammar is what is refused, not the name.
func objectCommentRefusals(note string) []experiment {
	experiments := make([]experiment, 0, len(objectCommentStatements))
	for _, form := range objectCommentStatements {
		experiments = append(experiments, acceptanceNote(form.key, nil, form.statement, note))
	}
	return experiments
}

// withObjectCommentsUndecided returns a copy of undecided that also declares
// every object-comment key undecidable, with the same reason for each, on a
// dialect where no statement could separate them.
func withObjectCommentsUndecided(
	undecided map[capability.Capability]string, reason string,
) map[capability.Capability]string {
	merged := maps.Clone(undecided)
	if merged == nil {
		merged = make(map[capability.Capability]string, len(objectCommentStatements))
	}
	for _, form := range objectCommentStatements {
		merged[form.key] = reason
	}
	return merged
}

package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withConstraintComments answers the constraint-comment key in a dialect's
// plan. The PostgreSQL family runs the experiment, and every other dialect
// records the refusal of the statement: none of them has a spelling that
// comments a constraint and reads it back where Ptah reads one
// (stokaro/ptah#3678).
func withConstraintComments(p plan, dialect string) plan {
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		p.experiments = append(p.experiments, constraintCommentExperiment(postgresFamilySpelling(dialect)))
	case platform.SQLServer:
		p.experiments = append(p.experiments, constraintCommentRefusal("T-SQL has no COMMENT ON statement at all; "+
			"SQL Server carries a comment as an extended property, which this key does not name"))
	case platform.SQLite:
		p.experiments = append(p.experiments, constraintCommentRefusal("SQLite has no COMMENT ON statement at all"))
	case platform.MySQL, platform.MariaDB:
		p.experiments = append(p.experiments, constraintCommentRefusal("MySQL and MariaDB have no COMMENT ON "+
			"statement, and a constraint takes no comment clause of its own"))
	case platform.ClickHouse:
		p.experiments = append(p.experiments, constraintCommentRefusal("ClickHouse has no COMMENT ON statement, "+
			"and a table constraint takes no comment clause"))
	case platform.Oracle:
		p.experiments = append(p.experiments, constraintCommentRefusal("Oracle comments a table, a column and a "+
			"few other objects, and has no COMMENT ON CONSTRAINT"))
	}
	return p
}

// constraintCommentExperiment asks the PostgreSQL family whether it stores a
// constraint's comment where the reader looks. The constraint is a CHECK
// because it is the one kind every member of the family creates, Spanner
// included, so a refusal is the comment's and not the constraint's.
func constraintCommentExperiment(t tableSpelling) experiment {
	return objectComment(capability.ConstraintComments, nil,
		[]string{t.table("ccm_t", "n int, CONSTRAINT ccm_pos CHECK (n > 0)", "n")},
		[]string{"COMMENT ON CONSTRAINT ccm_pos ON ccm_t IS '" + probeComment + "'"},
		"SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'ccm_t'::regclass AND conname = 'ccm_pos' "+
			"AND obj_description(oid, 'pg_constraint') = '"+probeComment+"'",
	)
}

// constraintCommentRefusal asks a dialect with no COMMENT ON CONSTRAINT the
// question, so its answer is a refusal the run recorded rather than a value a
// preset asserted. The statement names no object that exists because the
// grammar is what is refused, not the name.
func constraintCommentRefusal(note string) experiment {
	return acceptanceNote(capability.ConstraintComments, nil, "COMMENT ON CONSTRAINT ccm ON ccm_t IS 'probe'", note)
}

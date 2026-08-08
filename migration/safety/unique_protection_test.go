package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestClassifySchemaDiff_UniqueProtectionRemovalIsDestructive covers the loss of
// a uniqueness guarantee that is stated as an index removal.
//
// Replacing a database `UNIQUE KEY uq_users_email (email)` with a desired plain
// `index "uq_users_email"` deletes the guarantee that no two rows share an
// email, and the comparator states it in IndexesRemoved because the object is
// reported by the index catalog. Classified only as `indexes_removed`, the whole
// change was a warning: it passed `--check-destructive` and every drift
// threshold that keys on destructive findings, on PostgreSQL as well as MySQL
// and MariaDB.
func TestClassifySchemaDiff_UniqueProtectionRemovalIsDestructive(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesAdded:   []types.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		IndexesRemoved: []types.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		ConstraintBackedIndexRemovals: []types.IndexRef{
			{Name: "uq_users_email", TableName: "users"},
		},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(safety.HasDestructive(findings), qt.IsTrue, qt.Commentf("findings: %+v", findings))
	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "unique_protections_removed",
		Count:    1,
		Severity: safety.Destructive,
	})
	c.Assert(safety.Highest(findings), qt.Equals, safety.Destructive)
}

// TestClassifySchemaDiff_PlainIndexRemovalStaysAWarning is the control: dropping
// an index no constraint enforces costs query plans, not guarantees, and must
// not start failing a destructive gate.
func TestClassifySchemaDiff_PlainIndexRemovalStaysAWarning(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesRemoved: []types.IndexRef{{Name: "idx_users_email", TableName: "users"}},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(safety.HasDestructive(findings), qt.IsFalse, qt.Commentf("findings: %+v", findings))
	c.Assert(safety.Highest(findings), qt.Equals, safety.Warning)
}

// TestClassify_DropIndexOfAUniqueKeyIsDestructive covers the statement side of
// the same loss, which is where `--check-destructive` reads its verdict.
//
// On MySQL and MariaDB a unique key and its constraint are one catalog row, so
// the statement that removes a uniqueness guarantee is spelled exactly like the
// statement that removes an access path. The spelling cannot tell them apart;
// the planner marks the node that does.
func TestClassify_DropIndexOfAUniqueKeyIsDestructive(t *testing.T) {
	c := qt.New(t)

	node := ast.NewDropIndex("uq_users_email").
		SetTable("users").
		SetEnforcesUniqueConstraint()

	assessments := safety.Assess([]ast.Node{node})

	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].Reason, qt.Equals,
		"DROP INDEX removes the uniqueness a UNIQUE constraint enforces")
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsTrue)
}

// TestClassify_DropOfAPlainIndexStaysAWarning is the statement-level control for
// the mark: an unmarked DROP INDEX keeps the severity it always had.
func TestClassify_DropOfAPlainIndexStaysAWarning(t *testing.T) {
	c := qt.New(t)

	assessments := safety.Assess([]ast.Node{
		ast.NewDropIndex("idx_users_email").SetTable("users"),
	})

	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Warning)
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsFalse)
}

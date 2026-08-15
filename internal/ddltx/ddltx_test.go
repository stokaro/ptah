package ddltx_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/ddltx"
)

// TestClassOf_CoversEveryCapabilityDialect is the guard that makes the
// revision-completion matrix honest. The matrix can only cover the classes
// that exist, so a dialect with no class would drop out of it silently — a
// matrix reading as complete while a target has no stated contract is worse
// than no matrix at all (issue #999).
//
// capability.DefaultDialects is the authority for "a dialect Ptah has a
// default preset for": adding a dialect there without a class fails here.
func TestClassOf_CoversEveryCapabilityDialect(t *testing.T) {
	for _, dialect := range capability.DefaultDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(capability.ForDialect(dialect), qt.IsNotNil)
			c.Assert(ddltx.ClassOf(dialect), qt.Not(qt.Equals), ddltx.Unclassified)
		})
	}
}

// TestClassOf_KnowsNoDialectOutsideTheCapabilityPresets is the control for the
// test above. Without it, "every dialect is classified" could be satisfied by
// a catch-all arm that classifies everything, including targets Ptah has no
// preset for.
func TestClassOf_KnowsNoDialectOutsideTheCapabilityPresets(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "oracle", dialect: "oracle"},
		{name: "db2", dialect: "db2"},
		{name: "empty", dialect: ""},
		{name: "whitespace", dialect: "   "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(capability.ForDialect(test.dialect), qt.IsNil)
			c.Assert(ddltx.ClassOf(test.dialect), qt.Equals, ddltx.Unclassified)
		})
	}
}

func TestClassOf_AssignsTheMeasuredClass(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    ddltx.Class
	}{
		{name: "postgres", dialect: platform.Postgres, want: ddltx.Transactional},
		{name: "postgres alias pgx", dialect: "pgx", want: ddltx.Transactional},
		{name: "sqlite", dialect: platform.SQLite, want: ddltx.Transactional},
		{name: "sqlite alias sqlite3", dialect: "sqlite3", want: ddltx.Transactional},
		{name: "cockroachdb", dialect: platform.CockroachDB, want: ddltx.Transactional},
		{name: "yugabytedb", dialect: platform.YugabyteDB, want: ddltx.Transactional},
		{name: "spanner", dialect: platform.Spanner, want: ddltx.Transactional},
		{name: "sqlserver", dialect: platform.SQLServer, want: ddltx.Transactional},
		{name: "sqlserver alias mssql", dialect: "mssql", want: ddltx.Transactional},
		{name: "mysql", dialect: platform.MySQL, want: ddltx.ImplicitCommit},
		{name: "mariadb", dialect: platform.MariaDB, want: ddltx.ImplicitCommit},
		{name: "clickhouse", dialect: platform.ClickHouse, want: ddltx.NoTransaction},
		{name: "clickhouse alias ch", dialect: "ch", want: ddltx.NoTransaction},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddltx.ClassOf(test.dialect), qt.Equals, test.want)
		})
	}
}

func TestBodySurvivesRevisionCompletionFailure(t *testing.T) {
	tests := []struct {
		name  string
		class ddltx.Class
		want  bool
	}{
		{name: "transactional rolls the body back", class: ddltx.Transactional, want: false},
		{name: "implicit commit keeps the body", class: ddltx.ImplicitCommit, want: true},
		{name: "no transaction keeps the body", class: ddltx.NoTransaction, want: true},
		{name: "unclassified promises nothing", class: ddltx.Unclassified, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddltx.BodySurvivesRevisionCompletionFailure(test.class), qt.Equals, test.want)
		})
	}
}

// TestAllStatementsDurable pins the distinction that
// BodySurvivesRevisionCompletionFailure does not draw. Both predicates are true
// for ClickHouse and they disagree about the MySQL family, which is the whole
// reason there are two of them.
func TestAllStatementsDurable(t *testing.T) {
	tests := []struct {
		name  string
		class ddltx.Class
		want  bool
	}{
		{name: "transactional keeps nothing", class: ddltx.Transactional, want: false},
		{name: "implicit commit keeps only a prefix", class: ddltx.ImplicitCommit, want: false},
		{name: "no transaction keeps everything", class: ddltx.NoTransaction, want: true},
		{name: "unclassified promises nothing", class: ddltx.Unclassified, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddltx.AllStatementsDurable(test.class), qt.Equals, test.want)
		})
	}
}

// TestAllStatementsDurableImpliesBodySurvives keeps the two predicates from
// drifting into a contradiction: a class where every statement is durable
// cannot be one whose body does not survive.
func TestAllStatementsDurableImpliesBodySurvives(t *testing.T) {
	for _, dialect := range capability.DefaultDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			class := ddltx.ClassOf(dialect)
			c.Assert(
				ddltx.AllStatementsDurable(class) && !ddltx.BodySurvivesRevisionCompletionFailure(class),
				qt.IsFalse,
			)
		})
	}
}

func TestHasCommitStep(t *testing.T) {
	tests := []struct {
		name  string
		class ddltx.Class
		want  bool
	}{
		{name: "transactional commits", class: ddltx.Transactional, want: true},
		{name: "implicit commit has nothing left to commit", class: ddltx.ImplicitCommit, want: false},
		{name: "no transaction has no commit", class: ddltx.NoTransaction, want: false},
		{name: "unclassified promises nothing", class: ddltx.Unclassified, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddltx.HasCommitStep(test.class), qt.Equals, test.want)
		})
	}
}

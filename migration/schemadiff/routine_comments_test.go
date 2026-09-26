package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// commentedRoutines declares an enum, a materialized view, a function, a
// procedure, a trigger and a policy in the schema app, each with comment.
func commentedRoutines(comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Enums:             []schemamodel.Enum{{Name: "e", Schema: "app", Values: []string{"a"}, Comment: comment}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "app.m", Body: "SELECT 1", Comment: comment}},
		Functions: []schemamodel.Function{
			{Name: "app.f", Parameters: "a integer", Returns: "integer", Language: "sql", Body: "SELECT 1", Comment: comment},
			{Name: "app.p", Kind: "PROCEDURE", Parameters: "a integer", Language: "sql", Body: "SELECT 1", Comment: comment},
		},
		Triggers: []schemamodel.Trigger{{
			Name: "tg", Table: "app.t", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW",
			ExecuteFunction: "app.touch", Comment: comment,
		}},
		RLSPolicies: []schemamodel.RLSPolicy{{Name: "pol", Table: "app.t", PolicyFor: "ALL", UsingExpression: "true", Comment: comment}},
	}
}

// reportedRoutines is the database side of commentedRoutines, as the
// PostgreSQL reader reports it: a routine carries the identity arguments the
// server records.
func reportedRoutines(comment string) *catalog.Database {
	return &catalog.Database{
		Enums:    []catalog.Enum{{Name: "e", Schema: "app", Values: []string{"a"}, Comment: comment}},
		MatViews: []catalog.MaterializedView{{Name: "m", Schema: "app", Body: "SELECT 1", Comment: comment}},
		Functions: []catalog.Function{
			{
				Name: "f", Schema: "app", Parameters: "a integer", IdentityArguments: new("a integer"),
				Returns: "integer", Language: "sql", Body: "SELECT 1", Comment: comment,
			},
			{
				Name: "p", Schema: "app", Kind: "PROCEDURE", Parameters: "IN a integer", IdentityArguments: new("IN a integer"),
				Language: "sql", Body: "SELECT 1", Comment: comment,
			},
		},
		Triggers: []catalog.Trigger{{
			Name: "tg", Schema: "app", Table: "t", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW",
			ExecuteFunction: "app.touch", Comment: comment,
		}},
		RLSPolicies: []catalog.RLSPolicy{{Name: "pol", Table: "app.t", PolicyFor: "ALL", UsingExpression: "true", Comment: comment}},
	}
}

// everyRoutineComment is the transition of every object commentedRoutines
// declares from current to desired, in the order the comparison sorts them.
// A routine is addressed by the identity arguments the server recorded, and
// a trigger and a policy by their table.
func everyRoutineComment(current, desired string) []difftypes.ObjectCommentChange {
	return []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedEnumType, Name: "app.e", Current: current, Desired: desired},
		{Kind: difftypes.CommentedFunction, Name: "app.f", Arguments: new("a integer"), Current: current, Desired: desired},
		{Kind: difftypes.CommentedMatView, Name: "app.m", Current: current, Desired: desired},
		{Kind: difftypes.CommentedPolicy, Name: "pol", Table: "app.t", Current: current, Desired: desired},
		{Kind: difftypes.CommentedProcedure, Name: "app.p", Arguments: new("IN a integer"), Current: current, Desired: desired},
		{Kind: difftypes.CommentedTrigger, Name: "tg", Table: "app.t", Current: current, Desired: desired},
	}
}

// A comment that differs on an enum, a materialized view, a routine, a
// trigger or a policy is a transition of its own (stokaro/ptah#3646).
func TestCompareWithDialect_RoutineAndRelationCommentDifferenceIsAChange(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		inDatabase string
		want       []difftypes.ObjectCommentChange
	}{
		{name: "a comment rewritten", declared: "new", inDatabase: "old", want: everyRoutineComment("old", "new")},
		{name: "a comment added", declared: "new", inDatabase: "", want: everyRoutineComment("", "new")},
		{name: "a comment removed from the declaration", declared: "", inDatabase: "old", want: everyRoutineComment("old", "")},
		{name: "the same comment on both sides", declared: "same", inDatabase: "same", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(commentedRoutines(test.declared), reportedRoutines(test.inDatabase), platform.Postgres)

			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}

// Each kind is compared only where the target stores its comment. CockroachDB
// 26.3 takes COMMENT ON FUNCTION and PROCEDURE and reads them back through
// pg_proc, and answers a syntax error to the other three statements; 25.4
// takes none of them.
func TestCompareWithDatabaseInfo_RoutineCommentsFollowTheTargetsCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    []difftypes.ObjectCommentChange
	}{
		{
			name:    "CockroachDB 25.4 stores none of them",
			dialect: platform.CockroachDB,
			caps:    capability.CockroachDB25(),
		},
		{
			name:    "CockroachDB 26.3 stores the routines'",
			dialect: platform.CockroachDB,
			caps:    capability.CockroachDB263(),
			want: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedFunction, Name: "app.f", Arguments: new("a integer"), Current: "old", Desired: "new"},
				{Kind: difftypes.CommentedProcedure, Name: "app.p", Arguments: new("IN a integer"), Current: "old", Desired: "new"},
			},
		},
		{
			name:    "PostgreSQL stores every one",
			dialect: platform.Postgres,
			caps:    capability.Postgres18(),
			want:    everyRoutineComment("old", "new"),
		},
		{
			// The two routine kinds are separate statements with separate
			// keys, and neither key answers for the other.
			name:    "a target that stores a function's comment and not a procedure's",
			dialect: platform.Postgres,
			caps:    capability.Postgres18().With(capability.ProcedureComments, false),
			want:    withoutKinds(everyRoutineComment("old", "new"), difftypes.CommentedProcedure),
		},
		{
			name:    "a target that stores a procedure's comment and not a function's",
			dialect: platform.Postgres,
			caps:    capability.Postgres18().With(capability.FunctionComments, false),
			want:    withoutKinds(everyRoutineComment("old", "new"), difftypes.CommentedFunction),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				commentedRoutines("new"),
				reportedRoutines("old"),
				catalog.ServerInfo{Dialect: test.dialect, Capabilities: test.caps},
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}

// overloadedRoutines declares two overloads of app.f, the first with comment.
func overloadedRoutines(comment string) *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "app.f", Parameters: "a integer", Returns: "integer", Language: "sql", Body: "SELECT 1", Comment: comment},
		{Name: "app.f", Parameters: "a text", Returns: "integer", Language: "sql", Body: "SELECT 1", Comment: "kept"},
	}}
}

// An overloaded function has one comment per overload. The transition names
// the overload whose comment changed by its arguments, and the other one,
// whose comment agrees, carries none.
func TestCompareWithDialect_OverloadCommentNamesItsOverload(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{Functions: []catalog.Function{
		{
			Name: "f", Schema: "app", Parameters: "a text", IdentityArguments: new("a text"),
			Returns: "integer", Language: "sql", Body: "SELECT 1", Comment: "kept",
		},
		{
			Name: "f", Schema: "app", Parameters: "a integer", IdentityArguments: new("a integer"),
			Returns: "integer", Language: "sql", Body: "SELECT 1", Comment: "old",
		},
	}}

	diff := schemadiff.CompareWithDialect(overloadedRoutines("new"), database, platform.Postgres)

	c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedFunction, Name: "app.f", Arguments: new("a integer"), Current: "old", Desired: "new"},
	})
}

// A function declared in a document and a procedure of the same name in the
// database are two objects, and neither comment is compared with the other.
func TestCompareWithDialect_RoutineCommentKeepsItsKind(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{Functions: []catalog.Function{{
		Name: "f", Schema: "app", Kind: "PROCEDURE", Parameters: "IN a integer", IdentityArguments: new("IN a integer"),
		Language: "sql", Body: "SELECT 1", Comment: "old",
	}}}

	diff := schemadiff.CompareWithDialect(overloadedRoutines("new"), database, platform.Postgres)

	c.Assert(diff.ObjectCommentsChanged, qt.HasLen, 0)
}

package pgdefaultprivilege_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestComposeDeclaration_CombinesIndependentGrants(t *testing.T) {
	c := qt.New(t)
	left := &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "select"}}, Comment: "Read access"}
	right := &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}, {Privilege: "INSERT"}}, Revoked: []string{"delete"}}
	beforeLeft, beforeRight := left.Copy(), right.Copy()
	got, err := left.ComposeDeclaration(right)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, &pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}},
		Revoked:    []string{"DELETE"}, Comment: "Read access",
	})
	reverse, err := right.ComposeDeclaration(left)
	c.Assert(err, qt.IsNil)
	c.Assert(reverse.Equal(got), qt.IsTrue)
	repeated, err := got.(*pgdefaultprivilege.Desired).ComposeDeclaration(left)
	c.Assert(err, qt.IsNil)
	c.Assert(repeated.Equal(got), qt.IsTrue)
	c.Assert(left, qt.DeepEquals, beforeLeft)
	c.Assert(right, qt.DeepEquals, beforeRight)
}

func TestComposeDeclaration_RefusesConflicts(t *testing.T) {
	tests := []struct {
		name        string
		left, right *pgdefaultprivilege.Desired
	}{
		{name: "same privilege", left: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}, right: &pgdefaultprivilege.Desired{Revoked: []string{"select"}}},
		{name: "grant all", left: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}, right: &pgdefaultprivilege.Desired{Revoked: []string{"SELECT"}}},
		{name: "revoke all", left: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}, right: &pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.left.ComposeDeclaration(test.right)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.IsNil)
			reverse, err := test.right.ComposeDeclaration(test.left)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(reverse, qt.IsNil)
		})
	}
}

func TestComposeDeclaration_RefusesDifferentComments(t *testing.T) {
	c := qt.New(t)
	got, err := (&pgdefaultprivilege.Desired{Comment: "first"}).ComposeDeclaration(&pgdefaultprivilege.Desired{Comment: "second"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(got, qt.IsNil)
}

func TestComposeDeclaration_RefusesInvalidOperands(t *testing.T) {
	tests := []struct {
		name  string
		left  *pgdefaultprivilege.Desired
		right schemaext.Value
	}{
		{name: "nil left", right: &pgdefaultprivilege.Desired{}},
		{name: "nil right", left: &pgdefaultprivilege.Desired{}, right: (*pgdefaultprivilege.Desired)(nil)},
		{name: "observed", left: &pgdefaultprivilege.Desired{}, right: &pgdefaultprivilege.Observed{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.left.ComposeDeclaration(test.right)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.IsNil)
		})
	}
}

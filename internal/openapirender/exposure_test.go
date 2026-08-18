package openapirender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/openapirender"
	"go.5x5.cz/ptah/internal/schemaexport"
)

func exposureSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true, APIExpose: "read"},
			{StructName: "User", Name: "email", Type: "TEXT", APIExpose: "read-write"},
			{StructName: "User", Name: "password_hash", Type: "TEXT", APIExpose: "write"},
			{StructName: "User", Name: "internal_state", Type: "TEXT", APIExpose: "none"},
			{StructName: "User", Name: "undeclared", Type: "TEXT"},
		},
	}
}

func renderExposure(c *qt.C, policy schemaexport.FieldPolicy) string {
	c.Helper()
	rendered, err := openapirender.Render(exposureSchema(), openapirender.Options{FieldPolicy: policy})
	c.Assert(err, qt.IsNil)
	return string(rendered.Data)
}

// TestExposureShapesBecomeReadOnlyAndWriteOnly pins the mapping onto OpenAPI's
// own vocabulary.
//
// One schema per table carries both directions here, so a column readable and
// not writable is marked readOnly rather than moved into a second document, and
// the mirror for write. That is 3.0's own answer to the question, and it keeps
// the single `required` list correct: the specification says the requirement
// applies to the direction the marker names.
func TestExposureShapesBecomeReadOnlyAndWriteOnly(t *testing.T) {
	tests := []struct {
		name   string
		marker string
		want   string
	}{
		{name: "a read-only column", marker: "id", want: "readOnly: true"},
		{name: "a write-only column", marker: "password_hash", want: "writeOnly: true"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			document := renderExposure(c, schemaexport.FieldPolicyAll)

			c.Assert(document, qt.Contains, test.marker)
			c.Assert(document, qt.Contains, test.want)
		})
	}
}

// TestExposureNoneIsAbsentFromTheDocument is the criterion that matters most: a
// column declared none is not marked, it is not there.
//
// readOnly and writeOnly are advisory shape declarations and a writeOnly
// property is still published. Only non-emission removes a column from a
// contract, which is why the model has a "none" at all.
func TestExposureNoneIsAbsentFromTheDocument(t *testing.T) {
	tests := []struct {
		name   string
		policy schemaexport.FieldPolicy
	}{
		{name: "under the default policy", policy: schemaexport.FieldPolicyAll},
		{name: "under the allowlist policy", policy: schemaexport.FieldPolicyAllowlist},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			document := renderExposure(c, test.policy)

			c.Assert(document, qt.Not(qt.Contains), "internal_state")
		})
	}
}

// TestExposureAllowlistWithholdsUndeclaredColumns pins the policy that makes an
// additive migration safe, and pins that the withheld column also leaves the
// required list.
//
// A document that requires a property it does not define does not load, so the
// two have to move together.
func TestExposureAllowlistWithholdsUndeclaredColumns(t *testing.T) {
	c := qt.New(t)

	permissive := renderExposure(c, schemaexport.FieldPolicyAll)
	restricted := renderExposure(c, schemaexport.FieldPolicyAllowlist)

	c.Assert(permissive, qt.Contains, "undeclared")
	c.Assert(restricted, qt.Not(qt.Contains), "undeclared")
}

// TestExposureReportsOnlyWhatItWithheld pins that the diagnostics describe the
// document a reader gets.
//
// The shared model reports per shape and this target publishes one schema
// carrying both, so a column the read shape withheld and the write shape kept
// IS in the document. Reporting it as omitted would describe something the
// reader can see is there.
func TestExposureReportsOnlyWhatItWithheld(t *testing.T) {
	c := qt.New(t)

	rendered, err := openapirender.Render(exposureSchema(),
		openapirender.Options{FieldPolicy: schemaexport.FieldPolicyAll})

	c.Assert(err, qt.IsNil)
	paths := make([]string, 0, len(rendered.Diagnostics))
	for _, diagnostic := range rendered.Diagnostics {
		paths = append(paths, diagnostic.Path)
	}
	c.Assert(paths, qt.DeepEquals, []string{"users.internal_state"})
}

// TestExposureUnchangedWithoutDeclarations is the non-interference control: a
// schema that declares no exposure exports exactly as it did before this
// existed, under the default policy.
func TestExposureUnchangedWithoutDeclarations(t *testing.T) {
	c := qt.New(t)
	plain := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}

	before, err := openapirender.Render(plain, openapirender.Options{})
	c.Assert(err, qt.IsNil)
	after, err := openapirender.Render(plain, openapirender.Options{FieldPolicy: schemaexport.FieldPolicyAll})
	c.Assert(err, qt.IsNil)

	c.Assert(string(after.Data), qt.Equals, string(before.Data))
	c.Assert(string(before.Data), qt.Not(qt.Contains), "readOnly")
	c.Assert(string(before.Data), qt.Not(qt.Contains), "writeOnly")
}

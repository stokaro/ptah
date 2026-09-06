package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRenderDisclosesAGrantorItCannotRepresent is what makes
// `schemamodel.Grant.GrantedBy`'s disposition true.
//
// The field is filled by the PostgreSQL and Oracle catalog readers, no renderer
// emits it, and `migration/schemadiff` never mentions it. On that measurement
// alone it reads as a gap -- a fact that should reach SQL and does not -- and
// `internal/schemacensus` recorded it as one, naming stokaro/ptah#2611.
//
// It is not a gap. Measured on PostgreSQL 18.6, `GRANT ... GRANTED BY <role>`
// succeeds only when that role IS the current user: being a member of it
// answers `ERROR: grantor must be current user`, and a `SET ROLE` first makes
// the same statement succeed. Emitting an observed grantor would therefore fail
// on every apply whose connecting role is not that exact role, which is the
// ordinary case -- owners make grants, a migration role applies schemas.
//
// So the field is classified `export`: what a generated document carries, or
// reports that it cannot. This warning is the "reports that it cannot", and it
// is the field's only reader. Delete it and the field becomes populated with
// nothing reading it, which the census calls a defect -- while the entry would
// go on claiming a disclosure that no longer happens. Nothing tested it before
// this.
func TestRenderDisclosesAGrantorItCannotRepresent(t *testing.T) {
	c := qt.New(t)

	rendered, err := atlashclrender.Render(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Roles:   []schemamodel.Role{{Name: "reader"}},
		Grants: []schemamodel.Grant{{
			Role:       "reader",
			Privileges: []string{"SELECT"},
			OnSchema:   "public",
			GrantedBy:  "owner_role",
		}},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(diagnosticMessages(rendered.Diagnostics), qt.Contains,
		"grantor metadata cannot be represented in HCL schema permission blocks")
	// The document itself says nothing about the grantor, which is the other
	// half: the warning exists because the bytes cannot carry it.
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "owner_role")
}

// TestRenderIsSilentAboutAGrantWithNoGrantor is the control.
//
// A warning emitted for every grant would satisfy the assertion above while
// saying nothing about the field, and would tell an operator their document
// lost something it never held.
func TestRenderIsSilentAboutAGrantWithNoGrantor(t *testing.T) {
	c := qt.New(t)

	rendered, err := atlashclrender.Render(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Roles:   []schemamodel.Role{{Name: "reader"}},
		Grants: []schemamodel.Grant{{
			Role:       "reader",
			Privileges: []string{"SELECT"},
			OnSchema:   "public",
		}},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(diagnosticMessages(rendered.Diagnostics), qt.Not(qt.Contains),
		"grantor metadata cannot be represented in HCL schema permission blocks")
}

// diagnosticMessages is what the render said, without the paths.
func diagnosticMessages(diagnostics []atlashclrender.Diagnostic) []string {
	messages := make([]string, len(diagnostics))
	for index, diagnostic := range diagnostics {
		messages[index] = diagnostic.Message
	}
	return messages
}

package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_DeniedGrant_FailurePath refuses a SQL Server DENY: no annotation
// writes one, and an export that left it out would give the grantee what the
// database withholds.
func TestRender_DeniedGrant_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Roles:        []schemamodel.Role{{StructName: "R", Name: "reader"}},
		DeniedGrants: []schemamodel.Grant{{StructName: "D", Role: "reader", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: the schema denies SELECT on TABLE dbo\.t to "reader", which a Go annotation cannot represent`)
	c.Assert(files, qt.IsNil)
}

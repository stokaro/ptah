package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRender_ADialectScopeIsReportedAsAnExportLoss holds the one thing that
// protects a scope from the exporter.
//
// Atlas HCL has no way to say an object belongs to a subset of targets, so the
// scope genuinely does not survive the export. That is tolerable only because
// it is reported: `ptah schema export --cleanup-go-annotations` deletes the Go
// annotations after writing the HCL, and a loss diagnostic is what turns that
// into ErrLossyCleanup. Without this diagnostic, cleanup would delete the only
// place a scope was ever written and the schema would silently start reaching
// every dialect again.
func TestRender_ADialectScopeIsReportedAsAnExportLoss(t *testing.T) {
	tests := []struct {
		name string
		db   *goschema.Database
		want []atlashclrender.Diagnostic
	}{
		{
			name: "a scoped function",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Functions: []goschema.Function{{
					StructName: "Fn", Name: "tenant_id", Returns: "text",
					Language: "sql", Body: "SELECT 'x'", Dialects: []string{"postgres"},
				}},
			},
			want: []atlashclrender.Diagnostic{{
				Severity: atlashclrender.SeverityWarning,
				Path:     "function.tenant_id",
				Message:  `dialect scope "postgres" is not represented in HCL`,
			}},
		},
		{
			name: "a scoped role naming several dialects",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Roles: []goschema.Role{{
					StructName: "Rol", Name: "app_reader", Inherit: true,
					Dialects: []string{"cockroachdb", "postgres"},
				}},
			},
			want: []atlashclrender.Diagnostic{{
				Severity: atlashclrender.SeverityWarning,
				Path:     "role.app_reader",
				Message:  `dialect scope "cockroachdb,postgres" is not represented in HCL`,
			}},
		},
		{
			name: "an unscoped schema reports no scope loss",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Roles: []goschema.Role{{
					StructName: "Rol", Name: "app_reader", Inherit: true,
				}},
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.Render(test.db)
			c.Assert(err, qt.IsNil)

			c.Assert(scopeLossDiagnostics(result.Diagnostics), qt.DeepEquals, test.want)
		})
	}
}

// scopeLossDiagnostics keeps only the diagnostics this test is about, so an
// unrelated loss reported by the same render does not have to be restated here.
func scopeLossDiagnostics(diagnostics []atlashclrender.Diagnostic) []atlashclrender.Diagnostic {
	var scoped []atlashclrender.Diagnostic
	for _, diagnostic := range diagnostics {
		scoped = append(scoped, keepScopeLoss(diagnostic)...)
	}
	return scoped
}

func keepScopeLoss(diagnostic atlashclrender.Diagnostic) []atlashclrender.Diagnostic {
	const marker = "dialect scope "
	isScopeLoss := len(diagnostic.Message) >= len(marker) && diagnostic.Message[:len(marker)] == marker
	return map[bool][]atlashclrender.Diagnostic{true: {diagnostic}}[isScopeLoss]
}

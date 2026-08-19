package goannotationexport

import (
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

const opaqueSQLBodyMessage = "raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete"

func opaqueSQLDiagnostics(
	db *goschema.Database,
	existing []atlashclrender.Diagnostic,
) []atlashclrender.Diagnostic {
	diagnosticPaths := make(map[string]struct{}, len(existing))
	for _, diagnostic := range existing {
		diagnosticPaths[diagnostic.Path] = struct{}{}
	}

	var diagnostics []atlashclrender.Diagnostic
	for _, function := range db.Functions {
		diagnostics = appendOpaqueSQLDiagnostic(diagnostics, diagnosticPaths, "functions."+function.Name, function.Body)
	}
	for _, view := range db.Views {
		diagnostics = appendOpaqueSQLDiagnostic(diagnostics, diagnosticPaths, "views."+view.Name, view.Body)
	}
	for _, view := range db.MaterializedViews {
		diagnostics = appendOpaqueSQLDiagnostic(diagnostics, diagnosticPaths, "materialized_views."+view.Name, view.Body)
	}
	for _, trigger := range db.Triggers {
		trigger.Canonicalize()
		diagnostics = appendOpaqueSQLDiagnostic(
			diagnostics,
			diagnosticPaths,
			atlashclrender.TriggerDiagnosticPath(trigger.Table, trigger.Name),
			trigger.Body,
		)
	}
	return diagnostics
}

func appendOpaqueSQLDiagnostic(
	diagnostics []atlashclrender.Diagnostic,
	existingPaths map[string]struct{},
	path,
	body string,
) []atlashclrender.Diagnostic {
	if body == "" {
		return diagnostics
	}
	if _, exists := existingPaths[path]; exists {
		return diagnostics
	}
	existingPaths[path] = struct{}{}
	return append(diagnostics, atlashclrender.Diagnostic{
		Severity: atlashclrender.SeverityWarning,
		Path:     path,
		Message:  opaqueSQLBodyMessage,
	})
}

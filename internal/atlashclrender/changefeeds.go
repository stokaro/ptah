package atlashclrender

import (
	"fmt"
	"strings"
)

// reportChangefeeds names every table whose YDB changefeeds the document
// leaves out, because HCL has no block for one. That makes the loss
// [ptah.run/internal/goannotationexport.ErrLossyCleanup] for `ptah schema
// export --cleanup-go-annotations`, which would otherwise delete the only
// place the changefeeds are declared. Reading the document back drops none
// either: the loader records that HCL cannot express one.
func (r *renderer) reportChangefeeds() {
	for _, table := range r.db.Tables {
		if len(table.Changefeeds) == 0 {
			continue
		}
		names := make([]string, len(table.Changefeeds))
		for i, changefeed := range table.Changefeeds {
			names[i] = changefeed.Name
		}
		message := fmt.Sprintf("changefeeds %s are not represented in HCL", strings.Join(names, ", "))
		if len(names) == 1 {
			message = fmt.Sprintf("changefeed %s is not represented in HCL", names[0])
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     fmt.Sprintf("table.%s", table.QualifiedName()),
			Message:  message,
		})
	}
}

// reportSecrets names every YDB secret the document leaves out, because HCL
// has no block for one. Like a changefeed's, the loss makes
// `--cleanup-go-annotations` refuse, and reading the document back drops no
// secret: the loader records that HCL cannot express one.
func (r *renderer) reportSecrets() {
	for _, secret := range r.db.Secrets {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     "secret." + secret.QualifiedName(),
			Message:  fmt.Sprintf("secret %s is not represented in HCL", secret.QualifiedName()),
		})
	}
}

// reportExternalObjects names every YDB external data source and external
// table the document leaves out, because HCL has no block for either, for the
// reasons [renderer.reportSecrets] gives.
func (r *renderer) reportExternalObjects() {
	for _, source := range r.db.ExternalDataSources {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     "external_data_source." + source.QualifiedName(),
			Message:  fmt.Sprintf("external data source %s is not represented in HCL", source.QualifiedName()),
		})
	}
	for _, table := range r.db.ExternalTables {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     "external_table." + table.QualifiedName(),
			Message:  fmt.Sprintf("external table %s is not represented in HCL", table.QualifiedName()),
		})
	}
}

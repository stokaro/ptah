package atlashclrender

func (r *renderer) reportStreamingQueries() {
	for _, query := range r.db.StreamingQueries {
		r.diagnostics = append(r.diagnostics, Diagnostic{Severity: SeverityWarning, Path: "streaming_query." + query.QualifiedName(), Message: "streaming query " + query.QualifiedName() + " is not represented in HCL"})
	}
}

package providerdiagnostic

// SchemaPosition is one end of a diagnostic's span in a SQL schema document:
// a zero-based byte offset, and a one-based line and byte column.
type SchemaPosition struct {
	Offset uint64 `json:"offset"`
	Line   uint32 `json:"line"`
	Column uint32 `json:"column"`
}

// SchemaDiagnostic is one positioned diagnostic a provider reported about a
// SQL schema document it read whole. The message names a span and a short
// token; the provider never quotes the statement, so the document's text is
// not in it.
type SchemaDiagnostic struct {
	// Code is syntax, unsupported, invalid or incomplete.
	Code string `json:"code"`
	// Severity is error or warning.
	Severity string         `json:"severity"`
	Path     string         `json:"path"`
	Start    SchemaPosition `json:"start"`
	End      SchemaPosition `json:"end"`
	Message  string         `json:"message"`
}

// SchemaRefusal is a provider's refusal of the SQL schema documents of one
// load. Message is the sentence a command prints, which names the first error
// and its position; Diagnostics holds every diagnostic of the reply, in the
// provider's order, for a JSON report.
type SchemaRefusal struct {
	Message     string
	Diagnostics []SchemaDiagnostic
	// Cause classifies the refusal, such as an unsupported construct, for
	// errors.Is. It is not serialized.
	Cause error
}

// Error returns Message.
func (r *SchemaRefusal) Error() string { return r.Message }

// Unwrap returns Cause.
func (r *SchemaRefusal) Unwrap() error { return r.Cause }

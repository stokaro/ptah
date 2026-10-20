// Package providerdiagnostic carries engine-neutral installation failures.
// Shared reports preserve this data without importing the official catalog.
package providerdiagnostic

// Diagnostic preserves provider setup guidance in errors and JSON output.
// It contains no database address, credentials, or untrusted process output.
type Diagnostic struct {
	// Cause preserves cancellation and protocol error classification without serialization.
	Cause         error  `json:"-"`
	Code          string `json:"code"`
	EngineID      string `json:"engine_id"`
	ProviderID    string `json:"provider_id"`
	RepositoryURL string `json:"repository_url,omitempty"`
	SetupURL      string `json:"setup_url,omitempty"`
	Message       string `json:"message"`
}

// Error returns the human-readable diagnostic, including its setup link.
func (d *Diagnostic) Error() string { return d.Message }

// Unwrap preserves the underlying failure for cancellation and protocol checks.
func (d *Diagnostic) Unwrap() error { return d.Cause }

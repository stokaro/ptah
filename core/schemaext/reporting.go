package schemaext

import "context"

// ModelRuntime supplies the immutable codecs selected by an embedding caller.
// Stage-specific runtime interfaces add only the semantic services they consume.
type ModelRuntime interface {
	Codecs() Registry
}

// MetricDefinition describes an object-count metric owned by a feature model.
// Name is a lowercase ASCII metric suffix; Help is a single-line description.
// Counts describe captured schema values, not the contents of a live database.
type MetricDefinition struct {
	Name string
	Help string
}

// ReportDefinition names a feature for omission reports and declares its count
// metrics. Metadata is frozen during registration, not supplied by a service
// reply. DisplayName is plural, for example "changefeeds".
type ReportDefinition struct {
	Kind        Kind
	DisplayName string
	Metrics     []MetricDefinition
}

// MetricCount is one declared metric's contribution from a captured value.
// Value is nonnegative. Every declared metric must occur exactly once per value.
type MetricCount struct {
	Name  string
	Value int
}

// ValueReport describes one input value without publishing its payload. It
// retains the input kind and order. Subjects remain with the caller, which can
// apply reports to object or facet identities without parsing display strings.
type ValueReport struct {
	Kind   Kind
	Counts []MetricCount
}

// ReportingRequest supplies a complete ordered batch in one representation.
// Reporting reads captured data; it must not inspect a database or confer
// authority over unknown or absent schema state.
type ReportingRequest struct {
	// Target identifies the source target when known. It is optional context,
	// not a capability claim or a reporting-handler selector.
	Target         string
	Representation Representation
	Values         []Value
}

// ReportingService interprets captured values for inventory and export reports.
// A local implementation and a process adapter share this contextual batch
// contract. Failure or cancellation returns no usable partial report.
type ReportingService interface {
	ReportValues(context.Context, ReportingRequest) ([]ValueReport, error)
}

// FeatureReport contains selected model metadata and ordered value reports.
// Definitions also includes registered kinds absent from Values, allowing a
// consumer to report zero captured values without claiming inspected absence.
type FeatureReport struct {
	Definitions []ReportDefinition
	Values      []ValueReport
}

// ReportingRuntime validates and dispatches reports through selected providers.
// It snapshots inputs and rejects missing handlers, malformed replies, and
// unregistered metrics before returning any report.
type ReportingRuntime interface {
	ModelRuntime
	ReportFeatures(context.Context, ReportingRequest) (FeatureReport, error)
}

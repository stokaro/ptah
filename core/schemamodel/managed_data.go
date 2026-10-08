package schemamodel

// ManagedValue is one declared cell, kept as the YAML scalar that declared it
// rather than as the Go value that scalar resolves to.
//
// Resolution is lossy for the values a reference table is made of. Measured
// with go.yaml.in/yaml/v3: `007` resolves to int 7, `1.0` to float64 1, and
// `2020-01-01` to a time.Time. A published artifact has to carry what the
// author wrote, so Tag keeps the resolved YAML tag and Text the scalar's exact
// source text. The pair separates `007` from "007", which resolve to different
// SQL literals in the same column.
type ManagedValue struct {
	// Tag is the resolved YAML tag without its "!!" prefix: str, int, float,
	// bool, timestamp, or null.
	Tag string
	// Text is the scalar's source text. It is empty for a null.
	Text string
	// Null records a value the declaration spelled out as null, which is not
	// the same as a column the row never names.
	Null bool
}

// ManagedRow is one declared row: column name to declared value.
//
// A column absent from the map was not declared. A column mapped to a
// [ManagedValue] with Null set was declared null. The two are different
// statements about a row, and nothing downstream may collapse them.
type ManagedRow map[string]ManagedValue

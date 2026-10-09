package chast

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// AddSkippingIndexKind identifies a ClickHouse data-skipping index addition.
const AddSkippingIndexKind schemaext.Kind = "ptah.run/clickhouse/add-skipping-index"

// AddSkippingIndex adds index metadata to a MergeTree table. Expression and
// IndexType retain their SQL spelling and parameters. Empty IndexType selects
// minmax; zero Granularity selects one granule. This operation does not
// materialize the index for existing data.
type AddSkippingIndex struct {
	Name        string `json:"name"`
	Expression  string `json:"expression"`
	IndexType   string `json:"index_type"`
	Granularity int    `json:"granularity"`
}

// Kind returns the owned operation identity.
func (*AddSkippingIndex) Kind() schemaext.Kind { return AddSkippingIndexKind }

// CloneExtension returns an independent payload; a nil receiver stays typed nil.
func (v *AddSkippingIndex) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AddSkippingIndex)(nil)
	}
	return new(*v)
}

// Effect reports the additional work the index requires for subsequent writes.
func (*AddSkippingIndex) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "ADD INDEX changes future write work and query plans; existing data is not materialized by this operation"}
}

// SchemaChange reports the logical addition independently of its workload risk.
func (v *AddSkippingIndex) SchemaChange() ast.ExtensionChange {
	if v == nil {
		return ast.ExtensionChange{}
	}
	return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.Name}
}

// Validate requires an index name, an expression, and nonnegative granularity.
// Zero granularity and an empty type retain the documented default requests.
// SQL expression semantics remain the selected target's responsibility.
func (v *AddSkippingIndex) Validate() error {
	if v == nil || strings.TrimSpace(v.Name) == "" || strings.TrimSpace(v.Expression) == "" {
		return fmt.Errorf("%w: ClickHouse ADD INDEX requires a name and a non-empty expression", schemaext.ErrInvalidValue)
	}
	for _, value := range []string{v.Name, v.Expression, v.IndexType} {
		if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("%w: ClickHouse ADD INDEX contains invalid text", schemaext.ErrInvalidValue)
		}
	}
	if v.Granularity < 0 || v.IndexType != "" && strings.TrimSpace(v.IndexType) == "" {
		return fmt.Errorf("%w: ClickHouse ADD INDEX has invalid type or granularity", schemaext.ErrInvalidValue)
	}
	return nil
}

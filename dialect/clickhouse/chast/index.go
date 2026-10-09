package chast

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// AddSkippingIndexKind identifies a ClickHouse data-skipping index addition.
const AddSkippingIndexKind schemaext.Kind = "ptah.run/clickhouse/add-skipping-index"

// AddSkippingIndex adds index metadata to a MergeTree table. Expression and
// IndexType retain their SQL spelling and parameters. Empty IndexType selects
// minmax; zero Granularity selects one granule. Granularity carries the full
// unsigned 64-bit range ClickHouse accepts. This operation does not
// materialize the index for existing data.
type AddSkippingIndex struct {
	Name        string `json:"name"`
	Expression  string `json:"expression"`
	IndexType   string `json:"index_type"`
	Granularity uint64 `json:"granularity"`
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

// Validate requires an index name and an expression. Zero granularity and an
// empty type retain the documented default requests. SQL expression semantics
// remain the selected target's responsibility.
func (v *AddSkippingIndex) Validate() error {
	if v == nil || strings.TrimSpace(v.Name) == "" || strings.TrimSpace(v.Expression) == "" {
		return fmt.Errorf("%w: ClickHouse ADD INDEX requires a name and a non-empty expression", schemaext.ErrInvalidValue)
	}
	if !validText(v.Name, v.Expression, v.IndexType) {
		return fmt.Errorf("%w: ClickHouse ADD INDEX contains invalid text", schemaext.ErrInvalidValue)
	}
	if v.IndexType != "" && strings.TrimSpace(v.IndexType) == "" {
		return fmt.Errorf("%w: ClickHouse ADD INDEX has an invalid type", schemaext.ErrInvalidValue)
	}
	return nil
}

// DeclaredFacets returns the operation's settings as the skipping-index
// declaration a common index carries, bound to the clickhouse target. A
// nonempty type is explicit. Zero granularity is a SQL statement that left
// GRANULARITY out, which ClickHouse defines as one granule, so it requests the
// default rather than leaving the setting unmanaged; Ptah's own source formats
// keep an omitted setting unmanaged instead. An invalid operation returns its
// validation error and no facets.
func (v *AddSkippingIndex) DeclaredFacets() (schemaext.Facets, error) {
	if err := v.Validate(); err != nil {
		return schemaext.Facets{}, err
	}
	value := &chschema.DesiredIndex{}
	if v.IndexType != "" {
		value.IndexType = chschema.Setting{State: chschema.Explicit, Value: v.IndexType}
	}
	value.Granularity = chschema.GranularitySetting{State: chschema.Default}
	if v.Granularity != 0 {
		value.Granularity = chschema.GranularitySetting{State: chschema.Explicit, Value: v.Granularity}
	}
	facets, err := schemaext.NewFacets(value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(chschema.IndexKind, platform.ClickHouse)
}

// DropSkippingIndexKind identifies removal of a ClickHouse data-skipping index.
const DropSkippingIndexKind schemaext.Kind = "ptah.run/clickhouse/drop-skipping-index"

// DropSkippingIndex removes index metadata from a MergeTree table. The server
// discards the index data built for existing parts; table rows are unchanged.
type DropSkippingIndex struct {
	Name string `json:"name"`
}

// Kind returns the owned operation identity.
func (*DropSkippingIndex) Kind() schemaext.Kind { return DropSkippingIndexKind }

// CloneExtension returns an independent payload; a nil receiver stays typed nil.
func (v *DropSkippingIndex) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*DropSkippingIndex)(nil)
	}
	return new(*v)
}

// Effect reports the loss of built index data. Recreating the index restores
// its definition; MATERIALIZE INDEX is needed to index existing parts again.
func (*DropSkippingIndex) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "DROP INDEX discards the index data built for existing parts and changes query plans; table rows are unchanged"}
}

// SchemaChange reports the logical removal independently of its workload risk.
func (v *DropSkippingIndex) SchemaChange() ast.ExtensionChange {
	if v == nil {
		return ast.ExtensionChange{}
	}
	return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.Name}
}

// Validate requires a nonempty index name of valid text without NUL bytes.
func (v *DropSkippingIndex) Validate() error {
	if v == nil || strings.TrimSpace(v.Name) == "" || !validText(v.Name) {
		return fmt.Errorf("%w: ClickHouse DROP INDEX requires a valid index name", schemaext.ErrInvalidValue)
	}
	return nil
}

func validText(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
			return false
		}
	}
	return true
}

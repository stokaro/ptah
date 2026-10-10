package ydbast

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// DropVectorIndexKind identifies the removal of a vector index whose settings
// change.
const DropVectorIndexKind schemaext.Kind = "ptah.run/ydb/drop-vector-index"

// AddVectorIndexKind identifies the creation of a vector index with the
// settings a change asks for.
const AddVectorIndexKind schemaext.Kind = "ptah.run/ydb/add-vector-index"

// DropVectorIndex removes the vector index Name of the ALTER TABLE parent, so
// [AddVectorIndex] can build it again with other settings. YDB changes no
// setting of a built vector index in place.
type DropVectorIndex struct {
	Name string `json:"name"`
}

// Kind returns the stable operation identity.
func (*DropVectorIndex) Kind() schemaext.Kind { return DropVectorIndexKind }

// CloneExtension returns an independent operation; a nil receiver remains
// typed nil.
func (v *DropVectorIndex) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [DropVectorIndex.CloneExtension] without the interface. A nil
// receiver returns nil.
func (v *DropVectorIndex) Copy() *DropVectorIndex {
	if v == nil {
		return nil
	}
	return new(*v)
}

// Effect reports that the removal discards the index's tables, which the
// following [AddVectorIndex] builds again from the table's rows.
func (*DropVectorIndex) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "DROP INDEX discards the vector index until it is built again; nearest-neighbor searches through it fail meanwhile, and the table's rows are unchanged"}
}

// SchemaChange reports the removal of the named index.
func (v *DropVectorIndex) SchemaChange() ast.ExtensionChange {
	if v == nil {
		return ast.ExtensionChange{}
	}
	return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.Name}
}

// Validate requires a name.
func (v *DropVectorIndex) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: YDB vector index removal is nil", schemaext.ErrInvalidValue)
	}
	return validIndexName(v.Name)
}

// AddVectorIndex builds the vector index Name of the ALTER TABLE parent over
// Columns, the last of which holds the vectors, covering Cover, with
// Settings. Settings are resolved: they name every setting YDB requires.
type AddVectorIndex struct {
	Name     string                   `json:"name"`
	Columns  []string                 `json:"columns"`
	Cover    []string                 `json:"cover,omitempty"`
	Settings ydbschema.VectorSettings `json:"settings"`
}

// Kind returns the stable operation identity.
func (*AddVectorIndex) Kind() schemaext.Kind { return AddVectorIndexKind }

// CloneExtension returns independent operands; a nil receiver remains typed
// nil.
func (v *AddVectorIndex) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [AddVectorIndex.CloneExtension] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *AddVectorIndex) Copy() *AddVectorIndex {
	if v == nil {
		return nil
	}
	out := *v
	out.Columns = slices.Clone(v.Columns)
	out.Cover = slices.Clone(v.Cover)
	return &out
}

// Effect reports that building the index reads every row of the table.
func (*AddVectorIndex) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "ADD INDEX builds the vector index from every row of the table, which takes time and load on a large table"}
}

// SchemaChange reports the creation of the named index.
func (v *AddVectorIndex) SchemaChange() ast.ExtensionChange {
	if v == nil {
		return ast.ExtensionChange{}
	}
	return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.Name}
}

// Validate requires a name, at least one key column, column names that are
// not empty, and settings YDB builds an index with (see
// [ydbindex.ResolveVector]) that are already in the resolved form.
func (v *AddVectorIndex) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: YDB vector index creation is nil", schemaext.ErrInvalidValue)
	}
	if err := validIndexName(v.Name); err != nil {
		return err
	}
	if len(v.Columns) == 0 {
		return fmt.Errorf("%w: YDB vector index %q names no column", schemaext.ErrInvalidValue, v.Name)
	}
	for _, column := range slices.Concat(v.Columns, v.Cover) {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("%w: YDB vector index %q names an empty column", schemaext.ErrInvalidValue, v.Name)
		}
		if err := schemaext.ValidText("vector index column", column); err != nil {
			return err
		}
	}
	resolved, err := ydbindex.ResolveVector(&v.Settings, "")
	if err != nil {
		return fmt.Errorf("%w: YDB vector index %q: %w", schemaext.ErrInvalidValue, v.Name, err)
	}
	if resolved != v.Settings {
		return fmt.Errorf("%w: YDB vector index %q settings are not in resolved form", schemaext.ErrInvalidValue, v.Name)
	}
	return nil
}

// Clause is the index as ADD INDEX writes it after the name:
// `GLOBAL USING vector_kmeans_tree ON (...) [COVER (...)] WITH (...)`. quote
// writes one identifier.
func (v *AddVectorIndex) Clause(quote func(string) string) string {
	quoted := func(names []string) string {
		out := make([]string, len(names))
		for i, name := range names {
			out[i] = quote(name)
		}
		return strings.Join(out, ", ")
	}
	clause := ydbindex.Vector.Clause(false) + " ON (" + quoted(v.Columns) + ")"
	if len(v.Cover) > 0 {
		clause += " COVER (" + quoted(v.Cover) + ")"
	}
	return clause + " " + ydbindex.VectorClause(v.Settings)
}

func validIndexName(name string) error {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '/') {
		return fmt.Errorf("%w: a YDB index needs a name without a slash", schemaext.ErrInvalidValue)
	}
	return schemaext.ValidText("index name", name)
}

var (
	dropVectorIndexShape = schemaext.ObjectShape{Name: "YDB vector index removal", Allowed: []string{"name"}, Required: []string{"name"}}
	addVectorIndexShape  = schemaext.ObjectShape{Name: "YDB vector index creation",
		Allowed: []string{"name", "columns", "cover", "settings"}, Required: []string{"name", "columns", "settings"}, NonEmpty: []string{"cover"}}
)

// VectorIndexCodecs returns the version-one codecs of [DropVectorIndex] and
// [AddVectorIndex], in that order. The wire shape is the operation's own
// fields, never a serialized common Go AST; the settings take the owner's
// settings form. Every refusal is a [schemaext.InvalidModelError].
func VectorIndexCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DropVectorIndex]{
			Prototype: &DropVectorIndex{}, Representation: schemaext.Operation, Version: 1,
			Definition: json.RawMessage(`{"name":"the name of the vector index the ALTER TABLE parent drops"}`),
			Shape: func(data json.RawMessage) error {
				_, err := schemaext.DecodeObject(data, dropVectorIndexShape)
				return err
			},
			Validate: (*DropVectorIndex).Validate,
			Clone:    (*DropVectorIndex).Copy,
		}.Codec(),
		schemaext.ModelCodec[*AddVectorIndex]{
			Prototype: &AddVectorIndex{}, Representation: schemaext.Operation, Version: 1,
			Definition: json.RawMessage(fmt.Sprintf(`{"name":"the name of the vector index the ALTER TABLE parent adds",`+
				`"columns":"its key columns, the last holding the vectors","cover":"its covered columns, omitted when none",`+
				`"settings":%s,"constraint":"settings name every setting YDB requires, in lower case"}`, ydbschema.VectorIndexWireDefinition())),
			Shape: func(data json.RawMessage) error {
				fields, err := schemaext.DecodeObject(data, addVectorIndexShape)
				if err != nil {
					return err
				}
				for _, codec := range ydbschema.VectorIndexCodecs() {
					if codec.Representation == schemaext.Observed {
						_, err = codec.Decode(fields["settings"])
					}
				}
				return err
			},
			Validate: (*AddVectorIndex).Validate,
			Clone:    (*AddVectorIndex).Copy,
		}.Codec(),
	}
}

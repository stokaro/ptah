// Package mysqlast holds the operations the MySQL and MariaDB owner adds to a
// plan: what an ALTER TABLE statement does to one of the owner's settings, as
// operands a renderer writes without the schema. Its wire forms are the
// operations' own fields, never a serialized common Go AST.
package mysqlast

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ReplaceIndexKind identifies the replacement of an index whose block-size
// hint changes.
const ReplaceIndexKind schemaext.Kind = "ptah.run/mysql/replace-index"

// Index is an index definition as a MySQL-family ADD INDEX writes it.
type Index struct {
	// Name is the index name.
	Name string `json:"name"`
	// Unique makes the index UNIQUE.
	Unique bool `json:"unique,omitempty"`
	// Type is the declared index type: FULLTEXT and SPATIAL are written as a
	// prefix, HASH and BTREE as a USING clause, and any other is not written.
	Type string `json:"type,omitempty"`
	// Parts are the key parts, in order. There is at least one.
	Parts []IndexPart `json:"parts"`
	// Parser is the FULLTEXT parser plugin, empty for none.
	Parser string `json:"parser,omitempty"`
	// KeyBlockSize is the KEY_BLOCK_SIZE hint in kilobytes, zero for none.
	KeyBlockSize uint64 `json:"key_block_size,omitempty"`
	// Comment is the index comment, empty for none.
	Comment string `json:"comment,omitempty"`
	// Invisible hides the index from the optimizer.
	Invisible bool `json:"invisible,omitempty"`
}

// IndexPart is one key part: a column, with an optional prefix length, or an
// expression.
type IndexPart struct {
	Column     string `json:"column,omitempty"`
	Expression string `json:"expression,omitempty"`
	Prefix     string `json:"prefix,omitempty"`
	Descending bool   `json:"descending,omitempty"`
}

// Copy returns an index that shares no part with v.
func (v Index) Copy() Index {
	v.Parts = slices.Clone(v.Parts)
	return v
}

// Validate requires a name and at least one key part, each naming exactly one
// column or expression, and text without invalid UTF-8 or NUL.
func (v Index) Validate() error {
	if strings.TrimSpace(v.Name) == "" {
		return fmt.Errorf("%w: a MySQL index needs a name", schemaext.ErrInvalidValue)
	}
	if len(v.Parts) == 0 {
		return fmt.Errorf("%w: MySQL index %q has no key part", schemaext.ErrInvalidValue, v.Name)
	}
	for _, part := range v.Parts {
		if (part.Column == "") == (part.Expression == "") {
			return fmt.Errorf("%w: a key part of MySQL index %q names neither or both of a column and an expression", schemaext.ErrInvalidValue, v.Name)
		}
		for _, text := range []string{part.Column, part.Expression, part.Prefix} {
			if err := schemaext.ValidText("index key part", text); err != nil {
				return err
			}
		}
	}
	for _, text := range []string{v.Name, v.Type, v.Parser, v.Comment} {
		if err := schemaext.ValidText("index definition", text); err != nil {
			return err
		}
	}
	if v.KeyBlockSize != 0 {
		return mysqlschema.ValidateDesiredIndexBlockSize(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: v.KeyBlockSize})
	}
	return nil
}

// IndexFromNode returns the definition node holds: its name, uniqueness,
// type, key parts and comment, its visibility, and the parser and the
// block-size hint the MySQL owner's facets carry. Another active facet kind,
// and an invalid facet, are refused.
func IndexFromNode(node *ast.IndexNode) (Index, error) {
	if node == nil {
		return Index{}, fmt.Errorf("%w: no index to read a MySQL definition from", schemaext.ErrInvalidValue)
	}
	for _, kind := range node.Facets.Kinds() {
		if kind != mysqlschema.IndexKind && kind != mysqlschema.IndexBlockSizeKind {
			return Index{}, fmt.Errorf("%w: MySQL index facet %q is not supported", schemaext.ErrInvalidValue, kind)
		}
	}
	index := Index{Name: node.Name, Unique: node.Unique, Type: node.Type, Comment: node.Comment, Invisible: node.Invisible}
	options, _, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](node.Facets, mysqlschema.IndexKind)
	if err != nil {
		return Index{}, err
	}
	if options != nil {
		index.Parser = options.Parser
	}
	if index.KeyBlockSize, _, err = mysqlschema.IndexBlockSize(node.Facets); err != nil {
		return Index{}, err
	}
	for _, part := range node.EffectiveParts() {
		converted := IndexPart{Prefix: part.Prefix, Descending: part.Desc}
		if part.Expr != "" {
			converted.Expression = part.Expr
		} else {
			converted.Column = part.Name
		}
		index.Parts = append(index.Parts, converted)
	}
	return index, nil
}

// IndexFromModel returns the definition a declared index is created with: the
// one the common planner lowers it to, read as [IndexFromNode] reads a node.
// Key parts come from the index's parts and, where it has none, from its
// fields.
func IndexFromModel(index schemamodel.Index) (Index, error) {
	node := &ast.IndexNode{Facets: index.Facets, Name: index.Name, Columns: index.Fields, Unique: index.Unique,
		Type: index.Type, Comment: index.Comment, Invisible: index.Invisible}
	for _, part := range index.Parts {
		node.Parts = append(node.Parts, ast.IndexPart{Name: part.Name, Expr: part.Expr, Prefix: part.Prefix, Desc: part.Desc})
	}
	return IndexFromNode(node)
}

// ReplaceIndex drops the index Index.Name of the ALTER TABLE parent and adds
// it again with the definition Index, in one statement, so the table is never
// without it and an index a foreign key needs is not refused. It is how the
// owner applies a changed block-size hint, which no statement changes in
// place.
type ReplaceIndex struct {
	// Index is the definition the index is added with.
	Index Index `json:"index"`
	// TableCopy asks for ALGORITHM=COPY. MySQL needs it where nothing else in the
	// definition changes: measured on 8.4.11, an in-place DROP and ADD of the
	// same key keeps the old hint and reports success. MariaDB 11.8.9 stores
	// the new hint in place, so a MariaDB replacement leaves it false.
	TableCopy bool `json:"table_copy,omitempty"`
}

// Kind returns the stable operation identity.
func (*ReplaceIndex) Kind() schemaext.Kind { return ReplaceIndexKind }

// CloneExtension returns independent operands; a nil receiver remains typed
// nil.
func (v *ReplaceIndex) CloneExtension() ast.ExtensionPayload { return v.Clone() }

// Clone is [ReplaceIndex.CloneExtension] without the interface. A nil
// receiver returns nil.
func (v *ReplaceIndex) Clone() *ReplaceIndex {
	if v == nil {
		return nil
	}
	return &ReplaceIndex{Index: v.Index.Copy(), TableCopy: v.TableCopy}
}

// Effect reports the cost of the rebuild: with TableCopy, the whole table
// is copied.
func (v *ReplaceIndex) Effect() schemaext.Effect {
	if v != nil && v.TableCopy {
		return schemaext.Effect{Impact: schemaext.Behavioral,
			Reason: "ALGORITHM=COPY rebuilds the whole table to rebuild the index, which takes time on a large table and blocks writes to it while it runs; its rows are unchanged"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "the index is dropped and built again from every row of the table in one statement; its rows are unchanged"}
}

// SchemaChange reports a change to the named index.
func (v *ReplaceIndex) SchemaChange() ast.ExtensionChange {
	if v == nil {
		return ast.ExtensionChange{}
	}
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Index.Name}
}

// Validate requires a valid definition; see [Index.Validate].
func (v *ReplaceIndex) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: MySQL index replacement is nil", schemaext.ErrInvalidValue)
	}
	return v.Index.Validate()
}

var (
	replaceIndexShape = schemaext.ObjectShape{Name: "MySQL index replacement",
		Allowed: []string{"index", "table_copy"}, Required: []string{"index"}, NonEmpty: []string{"table_copy"}}
	indexShape = schemaext.ObjectShape{Name: "MySQL index definition",
		Allowed:  []string{"name", "unique", "type", "parts", "parser", "key_block_size", "comment", "invisible"},
		Required: []string{"name", "parts"},
		NonEmpty: []string{"unique", "type", "parser", "key_block_size", "comment", "invisible"}}
	indexPartShape = schemaext.ObjectShape{Name: "MySQL index key part",
		Allowed:  []string{"column", "expression", "prefix", "descending"},
		NonEmpty: []string{"column", "expression", "prefix", "descending"}}
)

// Codecs returns the version-one codec of [ReplaceIndex]. A decoder accepts
// only the spelling the encoder writes. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{schemaext.ModelCodec[*ReplaceIndex]{
		Prototype: &ReplaceIndex{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"index":{"name":"the index the ALTER TABLE parent drops and adds again",` +
			`"parts":"its key parts, each a column with an optional prefix length or an expression, and descending when so",` +
			`"unique":"true for a UNIQUE index","type":"the declared index type","parser":"the FULLTEXT parser",` +
			`"key_block_size":"the KEY_BLOCK_SIZE hint in kilobytes","comment":"the index comment",` +
			`"invisible":"true for an index the optimizer does not use"},"table_copy":"true to ask for ALGORITHM=COPY",` +
			`"constraint":"an option left out is false, empty or zero, and is omitted"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, replaceIndexShape)
			if err != nil {
				return err
			}
			if fields, err = schemaext.DecodeObject(fields["index"], indexShape); err != nil {
				return err
			}
			parts, err := schemaext.DecodeJSON[[]json.RawMessage](fields["parts"])
			if err != nil {
				return err
			}
			for _, part := range parts {
				if _, err := schemaext.DecodeObject(part, indexPartShape); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: (*ReplaceIndex).Validate,
		Clone:    (*ReplaceIndex).Clone,
	}.Codec()}
}

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
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlindex"
)

// ReplaceIndexKind identifies the replacement of an index whose block-size
// hint changes.
const ReplaceIndexKind schemaext.Kind = "ptah.run/mysql/replace-index"

// PrimaryKeyName is the name MySQL and MariaDB give the index of every
// primary key.
const PrimaryKeyName = "PRIMARY"

// Index is an index definition as a MySQL-family ADD INDEX writes it, or ADD
// PRIMARY KEY for the table's primary key.
type Index struct {
	// Name is the index name, [PrimaryKeyName] for a primary key.
	Name string `json:"name"`
	// Primary makes the definition the table's primary key, written PRIMARY
	// KEY with no name of its own. A primary key is not declared UNIQUE, has
	// columns for its parts, takes no FULLTEXT or SPATIAL kind and no parser,
	// and is always visible.
	Primary bool `json:"primary,omitempty"`
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
	if v.Primary {
		if err := v.validatePrimary(); err != nil {
			return err
		}
	}
	if v.KeyBlockSize != 0 {
		return mysqlschema.ValidateDesiredIndexBlockSize(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: v.KeyBlockSize})
	}
	return nil
}

// validatePrimary refuses what a primary key cannot be: named other than
// PRIMARY, declared UNIQUE, of a kind other than an access method, with a
// parser, hidden, or with an expression for a part. MySQL refuses the last
// with ERROR 3756.
func (v Index) validatePrimary() error {
	if v.Name != PrimaryKeyName || v.Unique || v.Parser != "" || v.Invisible || mysqlindex.KindOf(v.Type).Prefix() != "" {
		return fmt.Errorf("%w: MySQL primary key %q declares what a primary key does not have", schemaext.ErrInvalidValue, v.Name)
	}
	for _, part := range v.Parts {
		if part.Expression != "" {
			return fmt.Errorf("%w: a MySQL primary key takes no expression for a key part", schemaext.ErrInvalidValue)
		}
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

// PrimaryKeyFromNode returns the definition the PRIMARY KEY constraint node
// holds: its key parts, its access method and its comment, and the block-size
// hint the MySQL owner's facets carry. Another active facet kind, an invalid
// facet and a node of another constraint type are refused.
func PrimaryKeyFromNode(node *ast.ConstraintNode) (Index, error) {
	if node == nil || node.Type != ast.PrimaryKeyConstraint {
		return Index{}, fmt.Errorf("%w: no primary key to read a MySQL definition from", schemaext.ErrInvalidValue)
	}
	for _, kind := range node.Facets.Kinds() {
		if kind != mysqlschema.IndexBlockSizeKind {
			return Index{}, fmt.Errorf("%w: MySQL primary key facet %q is not supported", schemaext.ErrInvalidValue, kind)
		}
	}
	key := Index{Name: PrimaryKeyName, Primary: true, Type: node.UsingMethod, Comment: node.Comment}
	var err error
	if key.KeyBlockSize, _, err = mysqlschema.IndexBlockSize(node.Facets); err != nil {
		return Index{}, err
	}
	parts := node.ColumnParts
	if len(parts) == 0 {
		for _, column := range node.Columns {
			parts = append(parts, ast.ConstraintColumn{Name: column})
		}
	}
	for _, part := range parts {
		key.Parts = append(key.Parts, IndexPart{Column: part.Name, Expression: part.Expr, Prefix: part.Prefix, Descending: part.Desc})
	}
	return key, nil
}

// PrimaryKeyFromDeclaration returns the definition the declared table's
// primary key is created with, as the common planner lowers it and with the
// settings [schemacapture.PrimaryKeyFacets] reads: the first PRIMARY KEY
// constraint the declaration holds, or else the key the table declares in its
// primary key attributes, or else in its fields, in their order. A declaration
// without a key is refused.
func PrimaryKeyFromDeclaration(declaration schemacapture.TableDeclaration) (Index, error) {
	for _, constraint := range declaration.Constraints {
		if strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") {
			node := ast.NewPrimaryKeyConstraint(constraint.Columns...)
			node.UsingMethod, node.Comment, node.Facets = constraint.UsingMethod, constraint.Comment, constraint.Facets
			return PrimaryKeyFromNode(node)
		}
	}
	table := declaration.Table
	columns := table.PrimaryKey
	if len(columns) == 0 && len(table.PrimaryKeyParts) == 0 {
		for _, field := range declaration.Fields {
			if field.Primary {
				columns = append(columns, field.Name)
			}
		}
	}
	node := ast.NewPrimaryKeyConstraint(columns...)
	if len(table.PrimaryKeyParts) > 0 {
		node.Columns, node.ColumnParts = nil, nil
		for _, part := range table.PrimaryKeyParts {
			node.Columns = append(node.Columns, part.Name)
			node.ColumnParts = append(node.ColumnParts, ast.ConstraintColumn{Name: part.Name, Prefix: part.Prefix, Desc: part.Desc})
		}
	}
	if len(node.Columns) == 0 {
		return Index{}, fmt.Errorf("%w: table %q declares no primary key", schemaext.ErrInvalidValue, table.Name)
	}
	node.UsingMethod, node.Comment, node.Facets = table.PrimaryKeyMethod, table.PrimaryKeyComment, table.PrimaryKeyFacets
	return PrimaryKeyFromNode(node)
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
// without it and an index a foreign key needs is not refused. For a primary
// key it drops and adds the PRIMARY KEY, which AUTO_INCREMENT and incoming
// foreign keys need the same way. It is how the owner applies a changed
// block-size hint, which no statement changes in place.
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
	noun := "index"
	if v != nil && v.Index.Primary {
		noun = "primary key"
	}
	if v != nil && v.TableCopy {
		return schemaext.Effect{Impact: schemaext.Behavioral,
			Reason: "ALGORITHM=COPY rebuilds the whole table to rebuild the " + noun + ", which takes time on a large table and blocks writes to it while it runs; its rows are unchanged"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "the " + noun + " is dropped and built again from every row of the table in one statement; its rows are unchanged"}
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
		Allowed:  []string{"name", "primary", "unique", "type", "parts", "parser", "key_block_size", "comment", "invisible"},
		Required: []string{"name", "parts"},
		NonEmpty: []string{"primary", "unique", "type", "parser", "key_block_size", "comment", "invisible"}}
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
			`"primary":"true for the table's primary key, which is named PRIMARY",` +
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

package atlashcl

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tablelookup"
	"ptah.run/internal/tableref"
)

// ownerClaim is a block an owner's shape matched, waiting for the owner's
// declaration, and the block a refusal is positioned at. index is where the
// claimed index sits in the document's indexes, for an [hclext.IndexAttributes]
// claim, and table where the claim's table sits in the document's tables, for
// an [hclext.InTable] claim; each is -1 for a claim at another site.
// references are the [hclext.Ref] attributes the block writes, as written,
// which are resolved once every table of the document is read.
type ownerClaim struct {
	materialized *int
	routine      *int
	block        *hclsyntax.Block
	match        hclext.Match
	index        int
	table        int
	references   []writtenReference
}

// writtenReference is a [hclext.Ref] attribute as the block writes it: the
// reference read as text, waiting for the document's tables.
type writtenReference struct {
	attribute hclext.Attribute
	text      string
}

// claimOwnerBlock reads a block an owner's shape matches with the refusals
// every block gets -- its labels, its nested blocks, its unknown attributes
// and the kind of each attribute it takes -- and records it for the owner to
// read once the document is walked.
//
// The attributes are read in the order the shape lists them, and each as its
// kind: a [hclext.String] through stringAttr, which refuses any other value,
// and a [hclext.Text] through exprString, which writes any value as text. The
// owner receives the values and never the expressions, so this package's
// evaluation context stays its own.
func (p *parser) claimOwnerBlock(block *hclsyntax.Block, shape hclext.Shape) error {
	schema, name, err := p.ownerName(block, shape)
	if err != nil {
		return err
	}
	children, err := p.ownerChildren(block, shape, name)
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedAttrs(block, ownerAttributeNames(shape, shape.Block.Naming != hclext.JoinedName), block.Type); err != nil {
		return err
	}
	values, references, err := p.ownerValues(block, shape, name)
	if err != nil {
		return err
	}
	if _, written := block.Body.Attributes["schema"]; written && listsSchemaReference(shape) {
		// The naming read the reference already, and an attribute is
		// evaluated once: a print() call in it prints once.
		values["schema"] = hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "schema", Name: schema}}
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, index: -1, table: -1, references: references,
		match: hclext.Match{Shape: shape, Claim: hclext.Claim{Block: block.Type, Schema: schema, Name: name, Attributes: values,
			Children: children, At: blockAt(block)}}})
	return nil
}

// ownerChildren reads the blocks nested in block whose type is one of the
// shape's children, in document order, with the label count and attributes
// each child shape selects. A second named child of one type and name is
// refused at it. Every other nested block is an unknown name, refused or
// dropped as this package's own
// are; name names the block in a refusal.
func (p *parser) ownerChildren(block *hclsyntax.Block, shape hclext.Shape, name string) ([]hclext.ClaimChild, error) {
	var children []hclext.ClaimChild
	type key struct{ kind, name string }
	read := make(map[key]bool)
	for _, nested := range block.Body.Blocks {
		index := slices.IndexFunc(shape.Block.Children, func(child hclext.Child) bool { return child.Type == nested.Type })
		if index < 0 {
			if err := p.rejectUnsupportedBlock(nested, block.Type); err != nil {
				return nil, err
			}
			continue
		}
		child := shape.Block.Children[index]
		label := block.Type + " " + nested.Type
		childName, err := p.ownerChildName(nested, child, label)
		if err != nil {
			return nil, err
		}
		if !child.Unlabeled && read[key{nested.Type, childName}] {
			return nil, p.blockError(nested, "%s %q declares %s %q twice", block.Type, name, nested.Type, childName)
		}
		read[key{nested.Type, childName}] = true
		values, err := p.childValues(nested, child, label)
		if err != nil {
			return nil, err
		}
		children = append(children, hclext.ClaimChild{Type: nested.Type, Name: childName, Attributes: values, At: blockAt(nested)})
	}
	return children, nil
}

// childValues reads a child: it refuses a block nested in it and an attribute
// its shape does not list, as this package refuses them in its own blocks,
// and reads each attribute the child writes as its kind. label names the
// child in a refusal, such as "composite field".
func (p *parser) childValues(nested *hclsyntax.Block, child hclext.Child, label string) (map[string]hclext.Value, error) {
	if err := p.rejectNestedBlocks(nested, label); err != nil {
		return nil, err
	}
	supported := make(map[string]bool, len(child.Attributes))
	for _, attribute := range child.Attributes {
		supported[attribute.Name] = true
	}
	if err := p.rejectUnsupportedAttrs(nested, supported, label); err != nil {
		return nil, err
	}
	values := make(map[string]hclext.Value, len(child.Attributes))
	for _, attribute := range child.Attributes {
		if nested.Body.Attributes[attribute.Name] == nil {
			if attribute.Kind == hclext.Ref && !attribute.Optional {
				return nil, p.blockError(nested, "%s requires %s", label, attribute.Name)
			}
			continue
		}
		value, present, err := p.ownerValue(nested, label, attribute)
		if err != nil {
			return nil, err
		}
		if present {
			values[attribute.Name] = value
		}
	}
	return values, nil
}

// ownerName reads the schema and the name a block's labels give the object,
// as the shape's naming reads them. A shape that lists `schema` as a
// reference to a schema has the naming's schema read as one; see
// [parser.schemaReference].
func (p *parser) ownerName(block *hclsyntax.Block, shape hclext.Shape) (schema, name string, err error) {
	naming := shape.Block.Naming
	if naming == hclext.JoinedName {
		name, err = p.objectName(block, block.Type)
		return "", name, err
	}
	if listsSchemaReference(shape) {
		return p.referencedSchemaAndName(block)
	}
	schema, name, err = p.objectSchemaAndName(block, block.Type)
	if err != nil {
		return "", "", err
	}
	if ref, ok := tableref.Parse(name); ok && ref.Qualified && naming == hclext.SchemaAndQualifiedName &&
		strings.TrimSpace(schema) == "" {
		schema, name = ref.Schema, ref.Name
	}
	return schema, name, nil
}

// readsSchema reports whether a naming reads a `schema` attribute of its own.
func readsSchema(naming hclext.Naming) bool {
	return naming == hclext.SchemaAndName || naming == hclext.SchemaAndQualifiedName
}

// listsSchemaReference reports whether a shape lists its naming's `schema`
// as a reference to a schema.
func listsSchemaReference(shape hclext.Shape) bool {
	if !readsSchema(shape.Block.Naming) {
		return false
	}
	for _, attribute := range shape.Block.Attributes {
		if attribute.Name == "schema" && hclext.NamesSchema(attribute) {
			return true
		}
	}
	return false
}

// referencedSchemaAndName reads a block's name as [parser.objectSchemaAndName]
// does, its `schema` attribute read as a schema reference: one label is the
// name, two are the schema and the name, and an attribute that names another
// schema than the label is a conflict.
func (p *parser) referencedSchemaAndName(block *hclsyntax.Block) (schema, name string, err error) {
	attr, written := block.Body.Attributes["schema"]
	if written {
		if schema, err = p.schemaReference(block, attr); err != nil {
			return "", "", err
		}
	}
	switch len(block.Labels) {
	case 1:
		return schema, block.Labels[0], nil
	case 2:
		// A written `schema = ""` conflicts with a schema label too, as the
		// extension block always read it, where objectSchemaAndName ignores
		// an empty attribute.
		if written && schema != block.Labels[0] {
			return "", "", p.blockError(block, "%s %q schema label conflicts with schema attribute %q", block.Type, block.Labels[1], schema)
		}
		return block.Labels[0], block.Labels[1], nil
	default:
		return "", "", p.blockError(block, "%s block requires one or two name labels", block.Type)
	}
}

// schemaReference reads an attribute that names one schema: a string, a
// string template it evaluates, `schema.<name>` or `schema["<name>"]`. A
// schema is named by one label, so a qualified reference, a reference to
// another type, and any other expression are refused.
func (p *parser) schemaReference(block *hclsyntax.Block, attr *hclsyntax.Attribute) (string, error) {
	switch attr.Expr.(type) {
	case *hclsyntax.TemplateExpr, *hclsyntax.TemplateWrapExpr:
		value, diags := attr.Expr.Value(p.ctx)
		if !diags.HasErrors() && value.IsKnown() && !value.IsNull() && value.Type() == cty.String {
			return value.AsString(), nil
		}
	}
	if traversal, diags := hcl.AbsTraversalForExpr(attr.Expr); !diags.HasErrors() && len(traversal) == 2 &&
		traversal.RootName() == "schema" {
		if name, ok := traversalPart(traversal[1]); ok {
			return name, nil
		}
	}
	return "", p.blockError(block, "%s attribute %q must be a string template or exact schema reference", block.Type, attr.Name)
}

// ownerAttributeNames are the attributes a block of shape may write: the
// shape's, and `schema` where its naming reads one.
func ownerAttributeNames(shape hclext.Shape, withSchema bool) map[string]bool {
	supported := map[string]bool{"schema": withSchema, "dialect": len(shape.Block.Dialects) > 0}
	for _, attribute := range shape.Block.Attributes {
		supported[attribute.Name] = true
	}
	return supported
}

// claimOwnerTableBlock records a block inside table, which the walk appends
// to the document's tables at position once its blocks are read, for the
// owner that reads it. The block takes no labels.
func (p *parser) claimOwnerTableBlock(table *schemamodel.Table, position int, block *hclsyntax.Block, shape hclext.Shape) error {
	if len(block.Labels) != 0 {
		return p.blockError(block, "%s block does not accept labels", block.Type)
	}
	children, err := p.ownerChildren(block, shape, table.Name)
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedAttrs(block, ownerAttributeNames(shape, false), block.Type); err != nil {
		return err
	}
	values, references, err := p.ownerValues(block, shape, "")
	if err != nil {
		return err
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, index: -1, table: position, references: references,
		match: hclext.Match{Shape: shape, Claim: hclext.Claim{Block: block.Type, Attributes: values, Children: children, At: blockAt(block),
			Table: &hclext.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName}}}})
	return nil
}

// blockAt says where a block stands, as `<file>:<line>`.
func blockAt(block *hclsyntax.Block) string {
	return fmt.Sprintf("%s:%d", block.TypeRange.Filename, block.TypeRange.Start.Line)
}

// blockOrigin names a block in a refusal that has to name two: what it
// declares and where.
func (p *parser) blockOrigin(block *hclsyntax.Block, what string) string {
	return what + " at " + blockAt(block)
}

// claimOwnerIndex records an index block, the one at position in the
// document's indexes, for the owner that reads attributes of index blocks.
// Every index is claimed, whether or not it writes the owner's attributes,
// because its type may select the owner's reading; the owner's attributes were
// admitted on the block by rejectUnsupportedIndexAttrs.
func (p *parser) claimOwnerIndex(block *hclsyntax.Block, position int) error {
	shape, owned := p.owners.Lookup(hclext.IndexAttributes, "index")
	if !owned {
		return nil
	}
	index := p.db.Indexes[position]
	values, references, err := p.ownerValues(block, shape, index.Name)
	if err != nil {
		return err
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, index: position, table: -1, references: references,
		match: hclext.Match{Shape: shape, Claim: hclext.Claim{Block: block.Type, Name: index.Name, Attributes: values, At: blockAt(block),
			Index: &hclext.Index{Table: index.TableName, Type: index.Type, Operator: index.Operator}}}})
	return nil
}

// ownerValues reads the attributes of shape that block writes, each as its
// kind and in the order the shape lists them. A [hclext.Ref] attribute is
// required, and is returned as written for [parser.resolveReference]; name
// names the block in its refusal.
func (p *parser) ownerValues(block *hclsyntax.Block, shape hclext.Shape, name string) (map[string]hclext.Value, []writtenReference, error) {
	values := make(map[string]hclext.Value, len(shape.Block.Attributes))
	var references []writtenReference
	for _, attribute := range shape.Block.Attributes {
		attr, written := block.Body.Attributes[attribute.Name]
		if attribute.Kind == hclext.Ref && attribute.Optional && !written {
			continue
		}
		if attribute.Name == "schema" && hclext.NamesSchema(attribute) && readsSchema(shape.Block.Naming) {
			// The naming's own schema, which [parser.ownerName] read.
			continue
		}
		if hclext.NamesSchema(attribute) && written {
			// A schema is no relation of the document, so the reference is
			// read here rather than resolved against its tables.
			schema, err := p.schemaReference(block, attr)
			if err != nil {
				return nil, nil, err
			}
			values[attribute.Name] = hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: attribute.Refers, Name: schema}}
			continue
		}
		if attribute.Kind == hclext.Ref && attribute.Refers == "column" {
			value, _, err := p.ownerColumnValue(block, attribute)
			if err != nil {
				return nil, nil, err
			}
			values[attribute.Name] = value
			continue
		}
		if attribute.Kind == hclext.Ref {
			// The reference this package reads anywhere, so a policy's
			// `on` reads as a trigger's does; one to another type of
			// object reads as none.
			text := objectRefName(p.optionalRawExpr(block.Body.Attributes[attribute.Name]), attribute.Refers)
			if text == "" {
				return nil, nil, p.blockError(block, "%s %q requires %s", block.Type, name, attribute.Name)
			}
			references = append(references, writtenReference{attribute: attribute, text: text})
			continue
		}
		if block.Body.Attributes[attribute.Name] == nil {
			continue
		}
		// Each attribute is evaluated once: a print() call in it prints
		// once, as it does in a block of this package's own.
		value, present, err := p.ownerValue(block, block.Type, attribute)
		if err != nil {
			return nil, nil, err
		}
		if present {
			values[attribute.Name] = value
		}
	}
	return values, references, nil
}

// ownerValue reads one attribute as its kind: a [hclext.String] through
// stringAttr, a [hclext.Bool] through optionalBoolPtr, a [hclext.Refs]
// through referenceList, a [hclext.TypeName] through typeAttrString, as a
// column's type is read, a [hclext.SQL] as the SQL of an sql() call or else
// as Text, and a [hclext.Text] through exprString. label names the block in a
// refusal.
func (p *parser) ownerValue(block *hclsyntax.Block, label string, attribute hclext.Attribute) (hclext.Value, bool, error) {
	attr := block.Body.Attributes[attribute.Name]
	if (attribute.Kind == hclext.Ref || attribute.Kind == hclext.Refs) && attribute.Refers == "column" {
		return p.ownerColumnValue(block, attribute)
	}
	switch attribute.Kind {
	case hclext.String:
		text, err := p.stringAttr(block, attribute.Name, label)
		return hclext.Value{Kind: attribute.Kind, Text: text}, err == nil, err
	case hclext.Bool:
		value, err := p.optionalBoolPtr(block, attribute.Name, label)
		if err != nil || value == nil {
			return hclext.Value{}, false, err
		}
		return hclext.Value{Kind: attribute.Kind, Bool: *value}, true, nil
	case hclext.Refs:
		references, err := p.referenceList(block, attr, attribute.Refers)
		return hclext.Value{Kind: attribute.Kind, References: references}, err == nil, err
	case hclext.TypeName:
		return hclext.Value{Kind: attribute.Kind, Text: p.typeAttrString(attr)}, true, nil
	case hclext.SQL:
		if sql, ok := p.sqlExpression(attr); ok {
			return hclext.Value{Kind: hclext.SQL, Text: sql}, true, nil
		}
		return hclext.Value{Kind: hclext.Text, Text: p.exprString(attr)}, true, nil
	default:
		return hclext.Value{Kind: attribute.Kind, Text: p.exprString(attr)}, true, nil
	}
}

// referenceList reads a [hclext.Refs] attribute: a list of references to
// objects of the type refers names, or a single one. `role.app`,
// `role["app"]` and `role.s.app` refer to it; an identifier or a string
// names it as written; a variable, a local or a function names it with the
// string it evaluates to. Any other element, such as a reference to another
// type of object, is refused at the attribute rather than read as a name.
//
// A string that spells a reference, such as "role.app", is refused at the
// element: this package once read it as the reference, and reading it now as
// the name it writes would grant to another role with nothing to tell the
// author. Refusing it costs a rare literal name one rewrite, and the meaning
// never changes under anyone.
func (p *parser) referenceList(block *hclsyntax.Block, attr *hclsyntax.Attribute, refers string) ([]hclext.Reference, error) {
	exprs := []hclsyntax.Expression{attr.Expr}
	if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok {
		exprs = tuple.Exprs
	}
	references := make([]hclext.Reference, 0, len(exprs))
	for _, expr := range exprs {
		reference, written, ok := p.referenceElement(expr, refers)
		if !ok {
			return nil, p.attributeError(attr, "%s attribute %q: %s is not a %s reference", block.Type, attr.Name, p.rawExprNode(expr), refers)
		}
		if err := p.refuseSpelledReference(block, attr, expr, written, refers); err != nil {
			return nil, err
		}
		references = append(references, reference)
	}
	return references, nil
}

// refuseSpelledReference refuses a name written as a string, at expr, that
// spells a reference to an object of refers, as "role.app" or "role[\"app\"]"
// does. The refusal suggests the reference written unquoted only where that
// is a reference this package reads.
func (p *parser) refuseSpelledReference(block *hclsyntax.Block, attr *hclsyntax.Attribute, expr hclsyntax.Expression, written, refers string) error {
	prefix := refers + "."
	name, spelled := strings.CutPrefix(written, prefix)
	if !spelled {
		prefix = refers + "["
		if name, spelled = bracketObjectRefName(written, refers); !spelled {
			return nil
		}
	}
	if unquoted, diags := hclsyntax.ParseExpression([]byte(written), "reference.hcl", hcl.InitialPos); !diags.HasErrors() {
		if _, _, readable := p.referenceElement(unquoted, refers); readable {
			return p.expressionError(expr, "%s attribute %q: %q is ambiguous; write %s unquoted for the %s %s, or name the %s without the %s prefix",
				block.Type, attr.Name, written, written, refers, name, refers, prefix)
		}
	}
	return p.expressionError(expr, "%s attribute %q: %q is ambiguous; name the %s without the %s prefix",
		block.Type, attr.Name, written, refers, prefix)
}

// referenceElement reads one element of a [hclext.Refs] attribute, with the
// string it was written as, if it was one, and false for one that is no
// reference to an object of refers.
func (p *parser) referenceElement(expr hclsyntax.Expression, refers string) (hclext.Reference, string, bool) {
	if mustEvaluate(expr) {
		return stringReference(expr, p.ctx, refers)
	}
	traversal, diags := hcl.AbsTraversalForExpr(expr)
	if diags.HasErrors() {
		return stringReference(expr, nil, refers)
	}
	if len(traversal) == 1 {
		return hclext.Reference{Kind: refers, Name: traversal.RootName()}, "", true
	}
	if traversal.RootName() != refers || len(traversal) > 3 {
		return hclext.Reference{}, "", false
	}
	parts := make([]string, 0, len(traversal)-1)
	for _, step := range traversal[1:] {
		part, ok := traversalPart(step)
		if !ok {
			return hclext.Reference{}, "", false
		}
		parts = append(parts, part)
	}
	if len(parts) == 1 {
		return hclext.Reference{Kind: refers, Name: parts[0]}, "", true
	}
	return hclext.Reference{Kind: refers, Schema: parts[0], Name: parts[1]}, "", true
}

// stringReference reads an expression that evaluates to a string in ctx as
// the name of an object of refers, with that string, and false for one that
// does not.
func stringReference(expr hclsyntax.Expression, ctx *hcl.EvalContext, refers string) (hclext.Reference, string, bool) {
	value, diags := expr.Value(ctx)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() || value.Type() != cty.String {
		return hclext.Reference{}, "", false
	}
	return hclext.Reference{Kind: refers, Name: value.AsString()}, value.AsString(), true
}

// resolveReference reads a [hclext.Ref] attribute as written into the
// object it refers to: a table the document declares, with that table's
// schema, and otherwise the schema and name the reference writes. A
// reference several declared tables match is refused, naming the block, and
// so is one that is no reference. The reference as written is returned with
// the refusal, so the owner can still check the block.
func (p *parser) resolveReference(claim ownerClaim, written writtenReference) (hclext.Reference, error) {
	refers := written.attribute.Refers
	asWritten := hclext.Reference{Kind: refers, Name: written.text}
	origin := p.blockOrigin(claim.block, claim.block.Type+" "+strconv.Quote(claim.match.Claim.Name))
	if refers == "table" {
		position, err := tablelookup.Declared(p.db.Tables, written.text)
		if err != nil {
			return asWritten, fmt.Errorf("%s: %w", origin, err)
		}
		if position >= 0 {
			table := p.db.Tables[position]
			return hclext.Reference{Kind: refers, Schema: table.Schema, Name: table.Name}, nil
		}
	}
	ref, ok := tableref.Parse(written.text)
	if !ok {
		return asWritten, fmt.Errorf("%s: %w: %q is not a %s reference", origin, ptaherr.ErrInvalidAttributeValue, written.text, refers)
	}
	return hclext.Reference{Kind: refers, Schema: ref.Schema, Name: ref.Name}, nil
}

// decodeOwnerClaims hands every claimed block to its owner, one call per
// owner, and returns what each declares. A refusal of one block is positioned
// at it, as a refusal of this package's own is. A refusal of the document as
// a whole -- a reference several declared tables match, or an owner's refusal
// of two blocks that declare one object -- names no block, so it is returned
// apart, as documentErr, for [ParseWithOptions] to report once this package
// has checked the whole document.
func (p *parser) decodeOwnerClaims() (declarations []hclext.Declaration, documentErr, err error) {
	if len(p.ownerClaims) == 0 {
		return nil, nil, nil
	}
	matches := make([]hclext.Match, 0, len(p.ownerClaims))
	var unresolved error
	for _, claim := range p.ownerClaims {
		match := claim.match
		if len(claim.references) > 0 {
			match.Claim.Attributes = maps.Clone(match.Claim.Attributes)
			for _, written := range claim.references {
				reference, err := p.resolveReference(claim, written)
				if unresolved == nil {
					unresolved = err
				}
				match.Claim.Attributes[written.attribute.Name] = hclext.Value{Kind: hclext.Ref, Reference: &reference}
			}
		}
		matches = append(matches, match)
	}
	// The loaders take no context yet. The owners read their blocks in
	// process, so the walk bounds the read.
	declarations, err = p.owners.Decode(context.Background(), matches)
	if refused, ok := errors.AsType[*hclext.ClaimError](err); ok {
		return nil, nil, p.blockError(p.ownerClaims[refused.Claim].block, "%v", refused.Err)
	}
	if documentErr = cmp.Or(unresolved, err); documentErr != nil {
		return nil, documentErr, nil
	}
	return declarations, nil, nil
}

// addOwnerDeclarations adds what each owner declared for its claims.
//
// A document declares each object once, as [parser.rejectRedeclarations]
// holds this package's own blocks to, so a second declaration is refused.
// Under [MergeRedeclarationsEnvVar] an object its owner states is idempotent
// to declare, such as an extension, is combined instead: an equal repeat is
// one declaration and one that differs is refused; see
// [schemaext.Objects.Declare].
func (p *parser) addOwnerDeclarations(declarations []hclext.Declaration) error {
	for i, declaration := range declarations {
		claim := p.ownerClaims[i]
		for _, object := range declaration.Objects {
			if err := p.addOwnerObject(claim, object, p.redeclarations); err != nil {
				return err
			}
		}
		if err := p.addOwnerFacets(claim, declaration.Facets); err != nil {
			return err
		}
		if declaration.TableSetting != nil {
			p.tableSettings = append(p.tableSettings, TableSetting{Schema: claim.match.Claim.Schema, Table: claim.match.Claim.Name,
				Block: claim.block.Type, Position: claim.block.TypeRange.String(), Value: declaration.TableSetting})
		}
	}
	return nil
}

// addOwnerObject adds one object a claim declares, refusing a second
// declaration of its identity, or combining it as a declaration where policy
// merges repeats. For an object its owner states is idempotent to declare,
// the refusal names the variable that merges repeats.
func (p *parser) addOwnerObject(claim ownerClaim, object schemaext.Object, policy redeclarationPolicy) error {
	add := p.db.FeatureObjects.With
	if policy.merge {
		add = p.db.FeatureObjects.Declare
	}
	objects, err := add(object)
	if err == nil {
		p.db.FeatureObjects = objects
		return nil
	}
	remedy := ""
	if redeclarable, ok := object.Value.(schemaext.Redeclarable); ok && redeclarable.Redeclarable() && !policy.merge {
		remedy = fmt.Sprintf(" (set %s=1 to merge repeated declarations instead)", MergeRedeclarationsEnvVar)
	}
	return p.blockError(claim.block, "%s %s is declared twice: %v%s", claim.block.Type, qualifiedName(claim.match.Claim), err, remedy)
}

// addOwnerFacets adds facets an owner declared to the index or the table its
// claim stands in.
func (p *parser) addOwnerFacets(claim ownerClaim, facets schemaext.Facets) error {
	switch {
	case facets.Len() == 0:
		return nil
	case claim.materialized != nil:
		view := &p.db.MaterializedViews[*claim.materialized]
		merged, err := view.Facets.Merge(facets)
		if err != nil {
			return p.blockError(claim.block, "materialized view %q: %v", view.Name, err)
		}
		view.Facets = merged
	case claim.routine != nil:
		routine := &p.db.Functions[*claim.routine]
		merged, err := routine.Facets.Merge(facets)
		if err != nil {
			return p.blockError(claim.block, "routine %q: %v", routine.Name, err)
		}
		routine.Facets = merged
	case claim.index >= 0:
		index := &p.db.Indexes[claim.index]
		merged, err := index.Facets.Merge(facets)
		if err != nil {
			return p.blockError(claim.block, "index %q: %v", index.Name, err)
		}
		index.Facets = merged
	case claim.table >= 0:
		table := &p.db.Tables[claim.table]
		merged, err := table.Facets.Merge(facets)
		if err != nil {
			return p.blockError(claim.block, "table %q: %v", table.Name, err)
		}
		table.Facets = merged
	}
	return nil
}

// qualifiedName names the object a claim declares as a refusal names it: its
// schema and name, or its name alone where its block names no schema.
func qualifiedName(claim hclext.Claim) string {
	if strings.TrimSpace(claim.Schema) == "" {
		return claim.Name
	}
	return claim.Schema + "." + claim.Name
}

// parseOwnerTopBlock selects a scoped model before reading its attributes.
// A known block with an unavailable dialect is refused even when unknown
// names are tolerated; dropping it would discard an explicit declaration.
func (p *parser) parseOwnerTopBlock(block *hclsyntax.Block) error {
	if !p.owners.Scoped(hclext.TopLevel, block.Type) {
		if shape, found := p.owners.Lookup(hclext.TopLevel, block.Type); found {
			return p.claimOwnerBlock(block, shape)
		}
		return p.rejectUnsupportedBlock(block, TopLevelScope)
	}
	dialect, err := p.ownerDialect(block)
	if err != nil {
		return err
	}
	shape, found := p.owners.ForDialect(dialect).Lookup(hclext.TopLevel, block.Type)
	if !found {
		return p.blockError(block, "%s has no owner for dialect %q", block.Type, dialect)
	}
	return p.claimOwnerBlock(block, shape)
}

// ownerDialect evaluates the block's selector once, keeping the caller's
// default only when the block leaves it out.
func (p *parser) ownerDialect(block *hclsyntax.Block) (string, error) {
	attribute, written := block.Body.Attributes["dialect"]
	if !written {
		return p.dialect, nil
	}
	dialect, err := p.stringAttr(block, "dialect", block.Type)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(dialect) == "" {
		return "", p.attributeError(attribute, "%s dialect must not be empty", block.Type)
	}
	return dialect, nil
}

// claimOwnerRoutine hands evaluated attributes to the owner of this block's
// routine attributes. The claim retains the exact declaration position.
func (p *parser) claimOwnerRoutine(block *hclsyntax.Block, position int) error {
	shape, owned := p.owners.Lookup(hclext.RoutineAttributes, block.Type)
	if !owned {
		return nil
	}
	routine := p.db.Functions[position]
	values, references, err := p.ownerValues(block, shape, routine.Name)
	if err != nil {
		return err
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, routine: new(position), index: -1, table: -1, references: references,
		match: hclext.Match{Shape: shape, Claim: hclext.Claim{Block: block.Type, Name: routine.Name, Attributes: values, At: blockAt(block)}}})
	return nil
}

// claimOwnerTable hands evaluated table attributes to their owner, retaining
// the declaration position so equal names in different schemas stay separate.
func (p *parser) claimOwnerTable(block *hclsyntax.Block, position int) error {
	shape, owned := p.owners.Lookup(hclext.TableAttributes, "table")
	if !owned {
		return nil
	}
	table := p.db.Tables[position]
	values, references, err := p.ownerValues(block, shape, table.Name)
	if err != nil {
		return err
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, index: -1, table: position, references: references,
		match: hclext.Match{Shape: shape, Claim: hclext.Claim{Block: block.Type, Schema: table.Schema, Name: table.Name,
			Attributes: values, At: blockAt(block), Table: &hclext.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName}}}})
	return nil
}

// claimOwnerMaterializedView hands evaluated attributes to the selected owner.
// The declaration position keeps equal names in separate schemas distinct.
func (p *parser) claimOwnerMaterializedView(block *hclsyntax.Block, position int) error {
	shape, owned := p.owners.Lookup(hclext.MaterializedViewAttributes, block.Type)
	if !owned {
		return nil
	}
	view := p.db.MaterializedViews[position]
	values, references, err := p.ownerValues(block, shape, view.Name)
	if err != nil {
		return err
	}
	p.ownerClaims = append(p.ownerClaims, ownerClaim{block: block, materialized: new(position), index: -1, table: -1, references: references,
		match: hclext.Match{Shape: shape, Claim: hclext.Claim{Block: block.Type, Name: view.Name, Attributes: values, At: blockAt(block)}}})
	return nil
}

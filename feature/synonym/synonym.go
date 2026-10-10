// Package synonym owns synonyms: schema-qualified aliases that SQL Server and
// Oracle resolve to another object. It holds the desired and observed models
// with their codecs, the change and the operation, every stage's service and
// the render handlers, which engine/builtin registers on the two targets that
// have the object. No other target knows what a synonym is.
//
// Ptah manages the alias and never its target. The target may be in another
// schema, another database or behind a linked server, and neither engine
// requires it to exist when the alias is created.
package synonym

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Owner is the provider identity the bundled runtime registers this owner
// under.
const Owner = "ptah.run/synonym"

// Kind identifies one synonym.
const Kind schemaext.Kind = "ptah.run/synonym/synonym"

// Synonym is one alias and the object it stands for.
type Synonym struct {
	// Schema is the schema the alias lives in; empty means the connection's
	// default schema.
	Schema string `json:"schema,omitempty"`
	// Name is the alias.
	Name string `json:"name"`
	// Target is the object the alias stands for, as one to four dot-separated
	// parts: server, database, schema and object, with the leading parts
	// optional. A part may be quoted in brackets, double quotes or backticks,
	// and a middle part may be empty, as in `srv..dbo.orders`.
	Target string `json:"target"`
}

// DesiredSynonym is a synonym a declaration asks for.
type DesiredSynonym struct {
	Synonym
	// Comment documents the declaration; a script writes it above the
	// statement and the server keeps nothing of it.
	Comment string `json:"comment,omitempty"`
	// StructName is the Go struct that declared the synonym, empty for another
	// source.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedSynonym is a synonym a read found. Its Target is in the spelling a
// declaration uses, see [DeclaredTarget].
type ObservedSynonym struct {
	Synonym
}

// Kind returns the owned identity.
func (*DesiredSynonym) Kind() schemaext.Kind { return Kind }

// Kind returns the owned identity.
func (*ObservedSynonym) Kind() schemaext.Kind { return Kind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredSynonym) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredSynonym)(nil)
	}
	clone := *v
	return &clone
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedSynonym) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedSynonym)(nil)
	}
	clone := *v
	return &clone
}

// Equal compares declarations field by field.
func (v *DesiredSynonym) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredSynonym)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations field by field.
func (v *ObservedSynonym) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedSynonym)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures an observation as a declaration of the same alias and
// target. A nil receiver remains nil.
func (v *ObservedSynonym) Desired() *DesiredSynonym {
	if v == nil {
		return nil
	}
	return &DesiredSynonym{Synonym: v.Synonym}
}

// Observed projects a declaration as the synonym its statement leaves.
func (v *DesiredSynonym) Observed() (*ObservedSynonym, error) {
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	return &ObservedSynonym{Synonym: v.Synonym}, nil
}

// QualifiedName is schema.name, or the name alone without a schema.
func (s Synonym) QualifiedName() string {
	if strings.TrimSpace(s.Schema) == "" {
		return s.Name
	}
	return s.Schema + "." + s.Name
}

// Ref is the identity of the alias. The schema and the name are folded to
// lower case: SQL Server's default collation compares names without case, and
// Oracle folds an unquoted name. The target is not part of the identity, so a
// changed target is a change of one synonym rather than a drop and an add of
// two.
func (s Synonym) Ref() objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(Kind), Schema: part(s.Schema), Name: part(s.Name)}
}

// SameTarget reports whether two targets name the same object under a
// connection's identifier rules: the same parts in the same positions,
// compared without their quoting and without case, with an absent schema part
// read as the default schema when the target names no database or server. SQL
// Server records a target with its own bracket quoting, so a declared
// `dbo.orders` and a stored `[dbo].[orders]` are one target, and Oracle records
// the owner a declaration may leave out, so a declared `orders` and a stored
// `APP.ORDERS` are one target for a connection whose default schema is APP.
func SameTarget(semantics identifier.Semantics, a, b string) bool {
	return targetKey(semantics, a) == targetKey(semantics, b)
}

func targetKey(semantics identifier.Semantics, target string) string {
	parts := TargetParts(target)
	if parts[0] == "" && parts[1] == "" && parts[2] == "" {
		parts[2] = semantics.DefaultSchema
	}
	return fold(strings.Join(parts[:], "."))
}

// TargetParts splits a target into its server, database, schema and object
// parts, counted from the right: the last part is always the object. A
// missing leading part is empty, and so is an empty middle part, which is how
// `srv..dbo.orders` names a linked server and no database. Each part loses one
// level of bracket, double-quote or backtick quoting.
func TargetParts(target string) [4]string {
	parts := splitQualified(target)
	var result [4]string
	for i := range 4 {
		index := len(parts) - 1 - i
		if index < 0 {
			break
		}
		result[3-i] = unquote(strings.TrimSpace(parts[index]))
	}
	return result
}

// DeclaredTarget writes target parts as a declaration spells them: the parts
// from the first one present to the object, joined by dots, with an empty
// middle part kept empty. A part holding a dot or a bracket is bracket-quoted,
// so that splitting the result gives the same parts back.
func DeclaredTarget(parts [4]string) string {
	first := 0
	for first < 3 && parts[first] == "" {
		first++
	}
	written := make([]string, 0, 4-first)
	for _, value := range parts[first:] {
		if strings.ContainsAny(value, ".[]\"`") {
			value = "[" + strings.ReplaceAll(value, "]", "]]") + "]"
		}
		written = append(written, value)
	}
	return strings.Join(written, ".")
}

// splitQualified splits a name on the dots outside a bracket, double-quote or
// backtick quoted part. Each part is a slice of the input, quotes included.
func splitQualified(name string) []string {
	var parts []string
	start := 0
	var closing byte
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case closing != 0 && c == closing && i+1 < len(name) && name[i+1] == closing:
			i++
		case closing != 0 && c == closing:
			closing = 0
		case closing != 0:
		case c == '[':
			closing = ']'
		case c == '"' || c == '`':
			closing = c
		case c == '.':
			parts = append(parts, name[start:i])
			start = i + 1
		}
	}
	return append(parts, name[start:])
}

// unquote removes one level of quoting from a part and undoubles the closing
// quote inside it. A part that is not quoted is returned as it is.
func unquote(part string) string {
	if len(part) < 2 {
		return part
	}
	for _, pair := range [][2]byte{{'[', ']'}, {'"', '"'}, {'`', '`'}} {
		if part[0] == pair[0] && part[len(part)-1] == pair[1] {
			closing := string(pair[1])
			return strings.ReplaceAll(part[1:len(part)-1], closing+closing, closing)
		}
	}
	return part
}

func part(value string) objectidentity.Part {
	if value == "" {
		return objectidentity.Part{}
	}
	return objectidentity.Part{Source: value, Normalized: fold(value)}
}

func fold(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

// Targets returns the targets that have synonyms, in a new slice.
func Targets() []string { return []string{platform.SQLServer, platform.Oracle} }

// supported reports whether target is one of [Targets].
func supported(target string) bool {
	switch platform.NormalizeDialect(target) {
	case platform.SQLServer, platform.Oracle:
		return true
	default:
		return false
	}
}

// DesiredObject records one declared synonym.
func DesiredObject(synonym DesiredSynonym) schemaext.Object {
	return schemaext.Object{Ref: synonym.Ref(), Value: synonym.Clone()}
}

// DeclaredObject records one synonym a source declares, bound to the targets
// that have synonyms, so a schema rendered or planned for another target
// leaves it out rather than refusing it.
func DeclaredObject(synonym DesiredSynonym) schemaext.Object {
	object := DesiredObject(synonym)
	object.Targets = Targets()
	return object
}

// ObservedObject records one synonym a read found.
func ObservedObject(synonym ObservedSynonym) schemaext.Object {
	return schemaext.Object{Ref: synonym.Ref(), Value: synonym.Clone()}
}

// ValidateDesired refuses a declaration no target can hold; see [Validate].
// Nil is invalid.
func ValidateDesired(v *DesiredSynonym) error {
	if v == nil {
		return invalid(schemaext.Desired, fmt.Errorf("%w: nil synonym declaration", schemaext.ErrInvalidValue))
	}
	if err := schemaext.ValidText("synonym comment", v.Comment); err != nil {
		return invalid(schemaext.Desired, err)
	}
	return invalid(schemaext.Desired, Validate(v.Synonym))
}

// ValidateObserved refuses an observation no target can report; see
// [Validate]. Nil is invalid.
func ValidateObserved(v *ObservedSynonym) error {
	if v == nil {
		return invalid(schemaext.Observed, fmt.Errorf("%w: nil synonym observation", schemaext.ErrInvalidValue))
	}
	return invalid(schemaext.Observed, Validate(v.Synonym))
}

// Validate refuses a synonym without a name or a target, a target of more
// than four parts or without its object part, and text that is not valid
// UTF-8 or holds a NUL. Errors wrap schemaext.ErrInvalidValue.
func Validate(s Synonym) error {
	for _, text := range []struct{ field, value string }{
		{"synonym schema", s.Schema}, {"synonym name", s.Name}, {"synonym target", s.Target},
	} {
		if err := schemaext.ValidText(text.field, text.value); err != nil {
			return err
		}
	}
	switch {
	case strings.TrimSpace(s.Name) == "":
		return fmt.Errorf("%w: a synonym needs a name", schemaext.ErrInvalidValue)
	case strings.TrimSpace(s.Target) == "":
		return fmt.Errorf("%w: synonym %q needs a target", schemaext.ErrInvalidValue, s.QualifiedName())
	case len(splitQualified(s.Target)) > 4:
		return fmt.Errorf("%w: synonym %q target %q has more than four parts", schemaext.ErrInvalidValue, s.QualifiedName(), s.Target)
	case TargetParts(s.Target)[3] == "":
		return fmt.Errorf("%w: synonym %q target %q names no object", schemaext.ErrInvalidValue, s.QualifiedName(), s.Target)
	}
	return nil
}

// ValidateRef requires the identity [Synonym.Ref] gives a synonym.
func ValidateRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(Kind) || ref.Name.Normalized == "" || !ref.Parent.Empty() || ref.Signature != "" {
		return fmt.Errorf("%w: invalid synonym identity %v", schemaext.ErrInvalidValue, ref)
	}
	return nil
}

func invalid(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: Kind, Representation: representation, Message: err.Error()}
}

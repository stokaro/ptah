package pgtypesource

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/lexer"
)

func (t *rangeTokens) createDomain() (sqlext.Value, error) {
	if err := t.words("CREATE", "DOMAIN"); err != nil {
		return sqlext.Value{}, err
	}
	ref, err := t.reference()
	if err != nil {
		return sqlext.Value{}, err
	}
	ref.Kind = objectidentity.Kind(pgtype.DomainKind)
	if t.keyword("AS") {
		t.pos++
	}
	base, err := t.domainText(t.baseTypeBoundary)
	if err != nil {
		return sqlext.Value{}, err
	}
	value := &pgtype.DesiredDomain{BaseType: base}
	seen := make(map[string]bool)
	for t.current().Type != lexer.TokenEOF {
		clause := strings.ToUpper(t.current().Value)
		if t.keyword("NOT") {
			clause = "NULL"
		}
		if seen[clause] {
			return sqlext.Value{}, fmt.Errorf("domain %s declares %s more than once", pgtype.QualifiedName(ref), clause)
		}
		seen[clause] = true
		if err := t.domainClause(value); err != nil {
			return sqlext.Value{}, err
		}
	}
	object, err := pgtype.DesiredObject(ref, value)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: pgtype.CreateDomainOf(ref, value), Objects: []schemaext.Object{object}}, nil
}

func (t *rangeTokens) domainClause(value *pgtype.DesiredDomain) error {
	switch {
	case t.keyword("DEFAULT"):
		t.pos++
		text, err := t.domainText(t.defaultBoundary)
		value.DefaultExpr = text
		return err
	case t.keyword("NOT"):
		if err := t.words("NOT", "NULL"); err != nil {
			return err
		}
		value.NotNull = true
		return nil
	case t.keyword("NULL"):
		t.pos++
		return nil
	case t.keyword("CHECK"):
		t.pos++
		text, err := t.parenthesized("domain CHECK")
		value.Check = text
		return err
	default:
		return fmt.Errorf("unsupported domain clause %q at position %d", t.current().Value, t.offset+t.current().Start)
	}
}

// domainText keeps quoted tokens, arrays, function calls, and CASE expressions
// intact. Outside them, boundary ends the base type or the default; it is
// handed the position the text starts at.
func (t *rangeTokens) domainText(boundary func(first int) bool) (string, error) {
	start, end, depth, cases := t.current().Start, t.current().Start, 0, 0
	first := t.pos
	for t.current().Type != lexer.TokenEOF {
		if depth == 0 && cases == 0 && boundary(first) {
			break
		}
		if t.operator("(") || t.operator("[") {
			depth++
		}
		if t.operator(")") || t.operator("]") {
			depth--
		}
		if depth < 0 {
			return "", fmt.Errorf("unexpected %s in domain declaration", t.current().Value)
		}
		if t.keyword("CASE") {
			cases++
		}
		if t.keyword("END") && cases > 0 {
			cases--
		}
		end = t.current().End
		t.pos++
	}
	if depth != 0 || cases != 0 || end == start {
		return "", fmt.Errorf("expected a complete domain type or default at position %d", t.offset+start)
	}
	return strings.TrimSpace(t.text[start:end]), nil
}

// constraintBoundary reports a clause that ends a base type or a default.
func (t *rangeTokens) constraintBoundary() bool {
	return t.keyword("CHECK") || t.keyword("CONSTRAINT") || t.keyword("COLLATE") || t.keyword("DEFAULT")
}

// baseTypeBoundary ends a base type at a clause, NOT NULL or NULL included.
func (t *rangeTokens) baseTypeBoundary(_ int) bool {
	return t.constraintBoundary() || t.keyword("NOT") || t.keyword("NULL")
}

// defaultBoundary ends a default at a clause, and at a NULL or NOT NULL that
// is not part of the expression.
func (t *rangeTokens) defaultBoundary(first int) bool {
	if t.constraintBoundary() {
		return true
	}
	if t.pos == first {
		return false
	}
	// An operand NULL and IS [NOT] NULL belong to the default expression.
	previous := t.tokens[t.pos-1]
	if previous.MatchIdentifierValue("IS") || previous.MatchIdentifierValue("NOT") ||
		previous.MatchIdentifierValue("FROM") || previous.MatchIdentifierValue("AND") || previous.MatchIdentifierValue("OR") {
		return false
	}
	if previous.Type == lexer.TokenOperator && !previous.MatchOperatorValue(")") && !previous.MatchOperatorValue("]") {
		return false
	}
	return t.keyword("NULL") || t.keyword("NOT") && t.pos+1 < len(t.tokens) && t.tokens[t.pos+1].MatchIdentifierValue("NULL")
}

func (t *rangeTokens) parenthesized(label string) (string, error) {
	if err := t.punctuation("("); err != nil {
		return "", err
	}
	start, end, depth := t.current().Start, t.current().Start, 1
	for t.current().Type != lexer.TokenEOF {
		if t.operator("(") {
			depth++
		}
		if t.operator(")") {
			depth--
		}
		if depth == 0 {
			if end == start {
				return "", fmt.Errorf("%s requires an expression", label)
			}
			t.pos++
			return strings.TrimSpace(t.text[start:end]), nil
		}
		end = t.current().End
		t.pos++
	}
	return "", fmt.Errorf("unclosed %s", label)
}

func (t *rangeTokens) createComposite() (sqlext.Value, error) {
	if err := t.words("CREATE", "TYPE"); err != nil {
		return sqlext.Value{}, err
	}
	ref, err := t.reference()
	if err != nil {
		return sqlext.Value{}, err
	}
	ref.Kind = objectidentity.Kind(pgtype.CompositeKind)
	if err := t.words("AS"); err != nil {
		return sqlext.Value{}, err
	}
	if err := t.punctuation("("); err != nil {
		return sqlext.Value{}, err
	}
	value := &pgtype.DesiredComposite{}
	for !t.operator(")") {
		name, err := t.name("composite attribute name")
		if err != nil {
			return sqlext.Value{}, err
		}
		start := t.pos
		typ, err := t.optionValue("composite attribute " + name)
		if err != nil {
			return sqlext.Value{}, err
		}
		for _, token := range t.tokens[start:t.pos] {
			if token.MatchIdentifierValue("COLLATE") {
				return sqlext.Value{}, fmt.Errorf("composite attribute %q: COLLATE is not represented", name)
			}
		}
		value.Attributes = append(value.Attributes, pgtype.Attribute{Name: name, Type: typ})
		if !t.operator(",") {
			break
		}
		t.pos++
		if t.operator(")") {
			return sqlext.Value{}, fmt.Errorf("expected a composite attribute after comma")
		}
	}
	if err := t.punctuation(")"); err != nil {
		return sqlext.Value{}, err
	}
	if err := t.end(); err != nil {
		return sqlext.Value{}, err
	}
	object, err := pgtype.DesiredObject(ref, value)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: &pgtype.CreateComposite{Target: pgtype.TargetOf(ref), Attributes: value.Attributes}, Objects: []schemaext.Object{object}}, nil
}

func (t *rangeTokens) typeComment(pattern string, known map[objectidentity.Key]schemaext.Object) (sqlext.Value, error) {
	kind := "TYPE"
	if pattern == commentDomainPattern {
		kind = "DOMAIN"
	}
	if err := t.words("COMMENT", "ON", kind); err != nil {
		return sqlext.Value{}, err
	}
	ref, err := t.reference()
	if err != nil {
		return sqlext.Value{}, err
	}
	kinds := []schemaext.Kind{pgtype.RangeKind, pgtype.CompositeKind, pgtype.DomainKind}
	if pattern == commentDomainPattern {
		kinds = []schemaext.Kind{pgtype.DomainKind}
	}
	var object schemaext.Object
	found := false
	for _, model := range kinds {
		ref.Kind = objectidentity.Kind(model)
		if object, found = known[ref.Key()]; found {
			break
		}
	}
	if !found {
		return sqlext.Value{Declined: true}, nil
	}
	if err := t.words("IS"); err != nil {
		return sqlext.Value{}, err
	}
	comment, err := t.commentText()
	if err != nil {
		return sqlext.Value{}, err
	}
	value, err := commentedType(object.Value, comment)
	if err != nil {
		return sqlext.Value{}, err
	}
	object, err = pgtype.DesiredObject(object.Ref, value)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: &pgtype.Comment{Target: pgtype.TargetOf(object.Ref), Comment: comment}, Replaces: []schemaext.Object{object}}, nil
}

func commentedType(value schemaext.Value, comment string) (schemaext.Value, error) {
	switch typed := value.(type) {
	case *pgtype.DesiredDomain:
		result := typed.Copy()
		result.Comment = comment
		return result, nil
	case *pgtype.DesiredComposite:
		result := typed.Copy()
		result.Comment = comment
		return result, nil
	case *pgtype.DesiredRange:
		result := typed.Copy()
		result.Comment = comment
		return result, nil
	default:
		return nil, fmt.Errorf("%w: a type comment requires a declaration", schemaext.ErrInvalidValue)
	}
}

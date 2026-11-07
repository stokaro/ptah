package pgpolicysource

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/lexer"
)

func (t *policyTokens) create(known map[objectidentity.Key]schemaext.Object) (sqlext.Value, error) {
	if err := t.words("CREATE", "POLICY"); err != nil {
		return sqlext.Value{}, err
	}
	name, err := t.name("policy name")
	if err != nil {
		return sqlext.Value{}, err
	}
	if err = t.words("ON"); err != nil {
		return sqlext.Value{}, err
	}
	table, err := t.table()
	if err != nil {
		return sqlext.Value{}, err
	}
	policy, err := t.clauses()
	if err != nil {
		return sqlext.Value{}, err
	}
	ref := Ref(table.schema, table.name, name)
	if _, found := known[ref.Key()]; found {
		return sqlext.Value{}, fmt.Errorf("%w: policy %q on table %s is declared twice; keep one declaration", schemaext.ErrDuplicate, name, table.written)
	}
	object, err := pgpolicy.DesiredPolicyObject(ref, policy)
	if err != nil {
		return sqlext.Value{}, err
	}
	operation := &pgpolicy.PolicyOperation{Schema: table.schema, Table: table.name, Name: name, Change: pgpolicy.PolicyChange{
		After: policy.Copy(), Access: pgpolicy.PolicyAccess(nil, &policy, pgpolicy.ExpressionsDiffer),
	}}
	return sqlext.Value{Statement: operation, Objects: []schemaext.Object{object}}, nil
}

func (t *policyTokens) clauses() (pgpolicy.DesiredPolicy, error) {
	var policy pgpolicy.DesiredPolicy
	seen := make(map[string]bool)
	for t.current().Type != lexer.TokenEOF {
		word := strings.ToUpper(t.current().Value)
		if t.current().Type != lexer.TokenIdentifier {
			return policy, ptaherr.At(t.offset+t.current().Start, fmt.Errorf("unsupported CREATE POLICY clause"))
		}
		if seen[word] {
			return policy, fmt.Errorf("duplicate %s clause in CREATE POLICY", word)
		}
		seen[word] = true
		t.pos++
		if err := t.clause(&policy, word); err != nil {
			return policy, err
		}
	}
	return policy, pgpolicy.ValidateDesiredPolicy(&policy)
}

func (t *policyTokens) clause(policy *pgpolicy.DesiredPolicy, word string) error {
	switch word {
	case "AS":
		switch {
		case t.keyword("PERMISSIVE"):
			policy.Composition = pgpolicy.Permissive
		case t.keyword("RESTRICTIVE"):
			policy.Composition = pgpolicy.Restrictive
		default:
			return ptaherr.At(t.offset+t.current().Start, fmt.Errorf("expected PERMISSIVE or RESTRICTIVE after AS"))
		}
		t.pos++
	case "FOR":
		if t.current().Type != lexer.TokenIdentifier {
			return ptaherr.At(t.offset+t.current().Start, fmt.Errorf("expected command after FOR"))
		}
		policy.Command = pgpolicy.Command(strings.ToUpper(t.current().Value))
		t.pos++
	case "TO":
		roles, err := t.roles()
		if err != nil {
			return err
		}
		policy.Roles = roles
	case "USING":
		expr, err := t.expression("USING")
		if err != nil {
			return err
		}
		policy.Using = new(expr)
	case "WITH":
		if err := t.words("CHECK"); err != nil {
			return err
		}
		expr, err := t.expression("WITH CHECK")
		if err != nil {
			return err
		}
		policy.WithCheck = new(expr)
	default:
		// The word is the token before the cursor, and the refusal is about it.
		return ptaherr.At(t.offset+t.tokens[t.pos-1].Start, fmt.Errorf("unsupported CREATE POLICY clause: %s", word))
	}
	return nil
}

func (t *policyTokens) roles() ([]pgpolicy.RoleSelector, error) {
	var roles []pgpolicy.RoleSelector
	for {
		token := t.current()
		name, err := t.name("role after TO")
		if err != nil {
			return nil, err
		}
		if t.target == platform.CockroachDB {
			name = identifier.ComparisonUnicodeInsensitive.IdentityKey(name)
		}
		role := pgpolicy.RoleSelector{Name: name}
		if token.Type == lexer.TokenIdentifier && SpellsKeyword(token.Value) {
			role = pgpolicy.RoleSelector{Keyword: pgpolicy.RoleKeyword(strings.ToUpper(token.Value))}
		}
		roles = append(roles, role)
		if !t.operator(",") {
			return roles, nil
		}
		t.pos++
	}
}

func (t *policyTokens) comment(known map[objectidentity.Key]schemaext.Object) (sqlext.Value, error) {
	if err := t.words("COMMENT", "ON", "POLICY"); err != nil {
		return sqlext.Value{}, err
	}
	name, err := t.name("policy name")
	if err != nil {
		return sqlext.Value{}, err
	}
	if err = t.words("ON"); err != nil {
		return sqlext.Value{}, err
	}
	table, err := t.table()
	if err != nil {
		return sqlext.Value{}, err
	}
	if err = t.words("IS"); err != nil {
		return sqlext.Value{}, err
	}
	comment, err := t.commentText()
	if err != nil {
		return sqlext.Value{}, err
	}
	ref := Ref(table.schema, table.name, name)
	policy := &pgpolicy.DesiredPolicy{}
	if object, found := known[ref.Key()]; found {
		var ok bool
		policy, ok = object.Value.(*pgpolicy.DesiredPolicy)
		if !ok {
			return sqlext.Value{}, fmt.Errorf("%w: SQL policy declaration has value %T", schemaext.ErrInvalidValue, object.Value)
		}
		policy = policy.Copy()
	}
	policy.Comment = comment
	object, err := pgpolicy.DesiredPolicyObject(ref, *policy)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: &pgpolicy.PolicyCommentOperation{Schema: table.schema, Table: table.name, Name: name, Comment: comment}, Replaces: []schemaext.Object{object}}, nil
}

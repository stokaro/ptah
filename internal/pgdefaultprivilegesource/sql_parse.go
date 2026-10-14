package pgdefaultprivilegesource

import (
	"fmt"
	"strings"

	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/lexer"
)

func (t *privilegeTokens) parse() (*pgdefaultprivilege.Operation, error) {
	if err := t.words("ALTER", "DEFAULT", "PRIVILEGES"); err != nil {
		return nil, err
	}
	identity, err := t.scope()
	if err != nil {
		return nil, err
	}
	operation := &pgdefaultprivilege.Operation{Identity: identity}
	if err := t.action(operation); err != nil {
		return nil, err
	}
	operation.Privileges, err = t.privileges()
	if err != nil {
		return nil, err
	}
	operation.Identity.ObjectType, err = t.objectType()
	if err != nil {
		return nil, err
	}
	recipient := "FROM"
	if operation.Action == pgdefaultprivilege.GrantRights {
		recipient = "TO"
	}
	if err := t.words(recipient); err != nil {
		return nil, err
	}
	public := t.keyword("PUBLIC")
	operation.Identity.Grantee, err = t.roleName("grantee")
	if err != nil {
		return nil, err
	}
	if public || operation.Identity.Grantee == "public" {
		operation.Identity.Grantee = "PUBLIC"
	}
	if operation.Action == pgdefaultprivilege.GrantRights && t.keyword("WITH") {
		if err := t.words("WITH", "GRANT", "OPTION"); err != nil {
			return nil, err
		}
		operation.WithOption = true
	}
	if err := t.endOfStatement(); err != nil {
		return nil, err
	}
	if err := operation.Validate(); err != nil {
		return nil, fmt.Errorf("ALTER DEFAULT PRIVILEGES at position %d: %w", t.offset, err)
	}
	return operation, nil
}

func (t *privilegeTokens) scope() (pgdefaultprivilege.Identity, error) {
	identity := pgdefaultprivilege.Identity{}
	if !t.keyword("FOR") {
		return identity, fmt.Errorf("ALTER DEFAULT PRIVILEGES requires FOR ROLE: the grantor is part of the default's identity")
	}
	t.pos++
	if !t.keyword("ROLE") && !t.keyword("USER") {
		return identity, fmt.Errorf("expected ROLE or USER after FOR in ALTER DEFAULT PRIVILEGES at position %d", t.offset+t.current().Start)
	}
	t.pos++
	grantor, err := t.roleName("grantor role")
	if err != nil {
		return identity, err
	}
	identity.Grantor = grantor
	if t.keyword("IN") {
		if err := t.words("IN", "SCHEMA"); err != nil {
			return identity, err
		}
		identity.Schema, err = t.singleName("schema")
	}
	return identity, err
}

func (t *privilegeTokens) action(operation *pgdefaultprivilege.Operation) error {
	switch {
	case t.keyword("GRANT"):
		operation.Action = pgdefaultprivilege.GrantRights
		t.pos++
	case t.keyword("REVOKE"):
		operation.Action = pgdefaultprivilege.RevokeRights
		t.pos++
		if t.keyword("GRANT") {
			if err := t.words("GRANT", "OPTION", "FOR"); err != nil {
				return err
			}
			operation.Action = pgdefaultprivilege.RevokeGrantOptions
		}
	default:
		return fmt.Errorf("expected GRANT or REVOKE in ALTER DEFAULT PRIVILEGES at position %d", t.offset+t.current().Start)
	}
	return nil
}

func (t *privilegeTokens) privileges() ([]string, error) {
	var names []string
	for {
		token := t.current()
		if token.Type != lexer.TokenIdentifier || t.keyword("ON") {
			return nil, fmt.Errorf("expected default privilege name at position %d", t.offset+token.Start)
		}
		t.pos++
		name := strings.ToUpper(token.Value)
		if name == "ALL" && t.keyword("PRIVILEGES") {
			t.pos++
		}
		names = append(names, name)
		if !t.comma() {
			return names, t.words("ON")
		}
		t.pos++
	}
}

func (t *privilegeTokens) objectType() (string, error) {
	token := t.current()
	if token.Type != lexer.TokenIdentifier {
		return "", fmt.Errorf("expected default privilege object type at position %d", t.offset+token.Start)
	}
	t.pos++
	name := strings.ToUpper(token.Value)
	if name == "LARGE" {
		if err := t.words("OBJECTS"); err != nil {
			return "", err
		}
		name = "LARGE OBJECTS"
	}
	return name, nil
}

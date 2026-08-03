package parser

import (
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/lexer"
)

// This file holds the statement grammar for the PostgreSQL schema objects that
// Ptah's own renderer emits but its SQL frontend used to refuse: sequences,
// roles, grants, policies, ALTER TABLE ... ENABLE ROW LEVEL SECURITY and
// materialized views. Refusing them made `ptah schema render` unable to read
// back the SQL it had just written (issue #932).

// parseCreateSequence parses CREATE SEQUENCE [IF NOT EXISTS] name [options].
func (p *Parser) parseCreateSequence() (*ast.CreateSequenceNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "SEQUENCE"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	ifNotExists, err := p.parseOptionalIfNotExists()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	schema, name, err := p.parseSchemaQualifiedName("sequence name")
	if err != nil {
		return nil, err
	}

	sequence := ast.NewCreateSequence(name).SetSchema(schema)
	if ifNotExists {
		sequence.SetIfNotExists()
	}
	if err := p.parseSequenceOptions(sequence); err != nil {
		return nil, err
	}
	return sequence, nil
}

// parseSequenceOptions consumes the option list that follows a sequence name.
func (p *Parser) parseSequenceOptions(sequence *ast.CreateSequenceNode) error {
	for {
		p.skipWhitespace()
		if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon {
			return nil
		}
		if p.current.Type != lexer.TokenIdentifier {
			return fmt.Errorf("unsupported CREATE SEQUENCE option at position %d", p.current.Start)
		}
		keyword := strings.ToUpper(p.current.Value)
		p.advance()
		if err := p.applySequenceOption(sequence, keyword); err != nil {
			return err
		}
	}
}

// applySequenceOption applies one CREATE SEQUENCE option keyword.
func (p *Parser) applySequenceOption(sequence *ast.CreateSequenceNode, keyword string) error {
	switch keyword {
	case "AS":
		return p.applySequenceAs(sequence)
	case "INCREMENT", "START", "MINVALUE", "MAXVALUE", "CACHE":
		return p.applySequenceNumericOption(sequence, keyword)
	case "CYCLE":
		sequence.SetCycle(true)
		return nil
	case "NO":
		return p.applySequenceNoOption(sequence)
	case "OWNED":
		return p.applySequenceOwnedBy(sequence)
	default:
		return fmt.Errorf("unsupported CREATE SEQUENCE option: %s at position %d", keyword, p.current.Start)
	}
}

func (p *Parser) applySequenceAs(sequence *ast.CreateSequenceNode) error {
	p.skipWhitespace()
	asType, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected type after AS in CREATE SEQUENCE: %w", err)
	}
	sequence.SetAs(asType)
	return nil
}

// applySequenceNumericOption handles the options that take a single integer,
// including the optional BY / WITH filler word PostgreSQL allows.
func (p *Parser) applySequenceNumericOption(sequence *ast.CreateSequenceNode, keyword string) error {
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("BY") || p.current.MatchIdentifierValue("WITH") {
		p.advance()
		p.skipWhitespace()
	}
	value, err := p.parseSignedInteger()
	if err != nil {
		return fmt.Errorf("expected integer after %s in CREATE SEQUENCE: %w", keyword, err)
	}
	switch keyword {
	case "INCREMENT":
		sequence.SetIncrement(value)
	case "START":
		sequence.SetStart(value)
	case "MINVALUE":
		sequence.SetMinValue(value)
	case "MAXVALUE":
		sequence.SetMaxValue(value)
	case "CACHE":
		sequence.SetCache(value)
	}
	return nil
}

// applySequenceNoOption handles NO MINVALUE / NO MAXVALUE / NO CYCLE, all of
// which restate a PostgreSQL default and so leave the node unset.
func (p *Parser) applySequenceNoOption(sequence *ast.CreateSequenceNode) error {
	p.skipWhitespace()
	keyword, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected MINVALUE, MAXVALUE or CYCLE after NO: %w", err)
	}
	switch strings.ToUpper(keyword) {
	case "MINVALUE", "MAXVALUE":
		return nil
	case "CYCLE":
		sequence.SetCycle(false)
		return nil
	default:
		return fmt.Errorf("unsupported CREATE SEQUENCE option: NO %s at position %d", keyword, p.current.Start)
	}
}

func (p *Parser) applySequenceOwnedBy(sequence *ast.CreateSequenceNode) error {
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "BY"); err != nil {
		return fmt.Errorf("expected BY after OWNED: %w", err)
	}
	p.skipWhitespace()
	owner, err := p.parseQualifiedIdentifier("OWNED BY target")
	if err != nil {
		return err
	}
	if !strings.EqualFold(owner, "NONE") {
		sequence.SetOwnedBy(owner)
	}
	return nil
}

// parseSignedInteger reads an optionally signed integer literal. The lexer
// emits numbers as identifiers and a leading '-' as an operator, so the sign
// has to be reassembled here.
func (p *Parser) parseSignedInteger() (int64, error) {
	sign := int64(1)
	if p.current.MatchOperatorValue("-") {
		sign = -1
		p.advance()
	} else if p.current.MatchOperatorValue("+") {
		p.advance()
	}
	if p.current.Type != lexer.TokenIdentifier {
		return 0, fmt.Errorf("expected integer, got %s at position %d", p.current.Type, p.current.Start)
	}
	value, err := strconv.ParseInt(p.current.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q at position %d", p.current.Value, p.current.Start)
	}
	p.advance()
	return sign * value, nil
}

// parseCreateRole parses CREATE ROLE name [WITH] [attribute ...].
func (p *Parser) parseCreateRole() (*ast.CreateRoleNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "ROLE"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected role name: %w", err)
	}

	// ast.NewCreateRole defaults Inherit to true, matching PostgreSQL. A
	// rendered role always spells every attribute out, so the loop below
	// restates the default rather than relying on it.
	role := ast.NewCreateRole(name)
	if err := p.parseRoleOptions(role); err != nil {
		return nil, err
	}
	return role, nil
}

func (p *Parser) parseRoleOptions(role *ast.CreateRoleNode) error {
	for {
		p.skipWhitespace()
		if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon {
			return nil
		}
		if p.current.Type != lexer.TokenIdentifier {
			return fmt.Errorf("unsupported CREATE ROLE option at position %d", p.current.Start)
		}
		keyword := strings.ToUpper(p.current.Value)
		p.advance()
		if err := p.applyRoleOption(role, keyword); err != nil {
			return err
		}
	}
}

func (p *Parser) applyRoleOption(role *ast.CreateRoleNode, keyword string) error {
	if applyRoleFlag(role, keyword) {
		return nil
	}
	switch keyword {
	case "WITH":
		return nil
	case "ENCRYPTED", "UNENCRYPTED":
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "PASSWORD"); err != nil {
			return fmt.Errorf("expected PASSWORD after %s: %w", keyword, err)
		}
		return p.applyRolePassword(role)
	case "PASSWORD":
		return p.applyRolePassword(role)
	default:
		return fmt.Errorf("unsupported CREATE ROLE option: %s at position %d", keyword, p.current.Start)
	}
}

// applyRoleFlag sets the boolean role attributes, whose negative spellings are
// the NO-prefixed keywords PostgreSQL and Ptah's renderer both use.
func applyRoleFlag(role *ast.CreateRoleNode, keyword string) bool {
	switch keyword {
	case "LOGIN":
		role.SetLogin(true)
	case "NOLOGIN":
		role.SetLogin(false)
	case "SUPERUSER":
		role.SetSuperuser(true)
	case "NOSUPERUSER":
		role.SetSuperuser(false)
	case "CREATEDB":
		role.SetCreateDB(true)
	case "NOCREATEDB":
		role.SetCreateDB(false)
	case "CREATEROLE":
		role.SetCreateRole(true)
	case "NOCREATEROLE":
		role.SetCreateRole(false)
	case "INHERIT":
		role.SetInherit(true)
	case "NOINHERIT":
		role.SetInherit(false)
	case "REPLICATION":
		role.SetReplication(true)
	case "NOREPLICATION":
		role.SetReplication(false)
	default:
		return false
	}
	return true
}

func (p *Parser) applyRolePassword(role *ast.CreateRoleNode) error {
	p.skipWhitespace()
	if p.current.Type != lexer.TokenString {
		return fmt.Errorf("expected password literal at position %d", p.current.Start)
	}
	role.SetPassword(trimSQLStringLiteral(p.current.Value))
	p.advance()
	return nil
}

// parseGrantStatement parses GRANT priv[, ...] ON [objtype] name TO role
// [WITH GRANT OPTION].
func (p *Parser) parseGrantStatement() (*ast.GrantPrivilegeNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "GRANT"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	privileges, err := p.parseGrantPrivileges()
	if err != nil {
		return nil, err
	}

	objectType, objectName, err := p.parseGrantTarget()
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
		return nil, fmt.Errorf("expected TO after GRANT target: %w", err)
	}
	p.skipWhitespace()
	role, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected grantee role: %w", err)
	}

	grant := ast.NewGrantPrivilege(role, objectType, objectName, privileges)
	withOption, err := p.parseGrantOptionSuffix()
	if err != nil {
		return nil, err
	}
	return grant.SetWithOption(withOption), nil
}

func (p *Parser) parseGrantPrivileges() ([]string, error) {
	var privileges []string
	for {
		p.skipWhitespace()
		privilege, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected privilege name: %w", err)
		}
		privileges = append(privileges, strings.ToUpper(privilege))
		p.skipWhitespace()
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}
		if p.current.MatchIdentifierValue("ON") {
			p.advance()
			return privileges, nil
		}
		return nil, fmt.Errorf("expected ',' or ON in GRANT privileges at position %d", p.current.Start)
	}
}

// grantObjectTypes are the GRANT target kinds Ptah models. A GRANT that omits
// the keyword entirely targets a table, which is PostgreSQL's own default.
var grantObjectTypes = map[string]bool{"TABLE": true, "SCHEMA": true, "SEQUENCE": true}

func (p *Parser) parseGrantTarget() (objectType, objectName string, err error) {
	p.skipWhitespace()
	objectType = "TABLE"
	if p.current.Type == lexer.TokenIdentifier {
		keyword := strings.ToUpper(p.current.Value)
		if grantObjectTypes[keyword] {
			objectType = keyword
			p.advance()
			p.skipWhitespace()
		}
	}
	objectName, err = p.parseQualifiedIdentifier("GRANT object name")
	if err != nil {
		return "", "", err
	}
	return objectType, objectName, nil
}

func (p *Parser) parseGrantOptionSuffix() (bool, error) {
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("WITH") {
		return false, nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "GRANT"); err != nil {
		return false, fmt.Errorf("expected GRANT after WITH: %w", err)
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "OPTION"); err != nil {
		return false, fmt.Errorf("expected OPTION after WITH GRANT: %w", err)
	}
	return true, nil
}

// parseCreatePolicy parses CREATE POLICY name ON table [AS kind] [FOR command]
// [TO roles] [USING (expr)] [WITH CHECK (expr)].
func (p *Parser) parseCreatePolicy() (*ast.CreatePolicyNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "POLICY"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	name, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected policy name: %w", err)
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
		return nil, fmt.Errorf("expected ON after policy name: %w", err)
	}
	p.skipWhitespace()
	table, err := p.parseQualifiedIdentifier("policy table name")
	if err != nil {
		return nil, err
	}

	policy := ast.NewCreatePolicy(name, table)
	if err := p.parsePolicyClauses(policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (p *Parser) parsePolicyClauses(policy *ast.CreatePolicyNode) error {
	for {
		p.skipWhitespace()
		if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon {
			return nil
		}
		if p.current.Type != lexer.TokenIdentifier {
			return fmt.Errorf("unsupported CREATE POLICY clause at position %d", p.current.Start)
		}
		keyword := strings.ToUpper(p.current.Value)
		p.advance()
		if err := p.applyPolicyClause(policy, keyword); err != nil {
			return err
		}
	}
}

func (p *Parser) applyPolicyClause(policy *ast.CreatePolicyNode, keyword string) error {
	switch keyword {
	case "AS":
		// PERMISSIVE / RESTRICTIVE has no place in Ptah's IR, and Ptah's HCL
		// frontend rejects policy `as` for the same reason. Refuse rather than
		// accept and drop a clause that inverts what the policy means.
		p.skipWhitespace()
		kind, err := p.expectIdentifier()
		if err != nil {
			return fmt.Errorf("expected PERMISSIVE or RESTRICTIVE after AS: %w", err)
		}
		return fmt.Errorf("unsupported CREATE POLICY clause: AS %s at position %d", strings.ToUpper(kind), p.current.Start)
	case "FOR":
		p.skipWhitespace()
		command, err := p.expectIdentifier()
		if err != nil {
			return fmt.Errorf("expected command after FOR: %w", err)
		}
		policy.SetPolicyFor(strings.ToUpper(command))
		return nil
	case "TO":
		return p.applyPolicyRoles(policy)
	case "USING":
		expression, err := p.parseParenthesizedExpression("USING")
		if err != nil {
			return err
		}
		policy.SetUsingExpression(expression)
		return nil
	case "WITH":
		return p.applyPolicyWithCheck(policy)
	default:
		return fmt.Errorf("unsupported CREATE POLICY clause: %s at position %d", keyword, p.current.Start)
	}
}

func (p *Parser) applyPolicyRoles(policy *ast.CreatePolicyNode) error {
	var roles []string
	for {
		p.skipWhitespace()
		role, err := p.expectIdentifier()
		if err != nil {
			return fmt.Errorf("expected role after TO: %w", err)
		}
		roles = append(roles, role)
		p.skipWhitespace()
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}
		policy.SetToRoles(strings.Join(roles, ", "))
		return nil
	}
}

func (p *Parser) applyPolicyWithCheck(policy *ast.CreatePolicyNode) error {
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "CHECK"); err != nil {
		return fmt.Errorf("expected CHECK after WITH: %w", err)
	}
	expression, err := p.parseParenthesizedExpression("WITH CHECK")
	if err != nil {
		return err
	}
	policy.SetWithCheckExpression(expression)
	return nil
}

// parseParenthesizedExpression collects the text inside a balanced paren pair,
// which is how policy USING / WITH CHECK expressions are rendered.
func (p *Parser) parseParenthesizedExpression(label string) (string, error) {
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return "", fmt.Errorf("expected '(' after %s: %w", label, err)
	}
	var body strings.Builder
	depth := 1
	for {
		if p.current.Type == lexer.TokenEOF {
			return "", fmt.Errorf("unterminated %s expression", label)
		}
		if err := p.checkTimeout(); err != nil {
			return "", err
		}
		if p.current.MatchOperatorValue("(") {
			depth++
		} else if p.current.MatchOperatorValue(")") {
			depth--
			if depth == 0 {
				p.advance()
				return strings.TrimSpace(body.String()), nil
			}
		}
		body.WriteString(p.current.Value)
		p.advance()
	}
}

// parseCreateMaterializedView parses
// CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS <query>.
func (p *Parser) parseCreateMaterializedView() (*ast.CreateMaterializedViewNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "MATERIALIZED"); err != nil {
		return nil, err
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "VIEW"); err != nil {
		return nil, fmt.Errorf("expected VIEW after MATERIALIZED: %w", err)
	}
	p.skipWhitespace()
	if _, err := p.parseOptionalIfNotExists(); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	name, err := p.parseQualifiedIdentifier("materialized view name")
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return nil, fmt.Errorf("expected AS after materialized view name: %w", err)
	}

	body := p.collectStatementBody()
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("expected materialized view body after AS at position %d", p.current.Start)
	}
	return ast.NewCreateMaterializedView(name).SetBody(body), nil
}

// parseAlterTableRowLevelSecurity parses the ROW LEVEL SECURITY tail of an
// ALTER TABLE ... ENABLE / DISABLE statement, with the ENABLE or DISABLE
// keyword already consumed. DISABLE has no IR representation, so it is refused
// rather than read as its opposite.
func (p *Parser) parseAlterTableRowLevelSecurity(table, keyword string, start int) (*ast.AlterTableEnableRLSNode, error) {
	for _, word := range []string{"ROW", "LEVEL", "SECURITY"} {
		if err := p.expect(lexer.TokenIdentifier, word); err != nil {
			return nil, fmt.Errorf("expected %s in ALTER TABLE ... %s ROW LEVEL SECURITY: %w", word, keyword, err)
		}
	}
	if keyword != "ENABLE" {
		return nil, fmt.Errorf("unsupported ALTER operation: %s ROW LEVEL SECURITY at position %d", keyword, start)
	}
	return ast.NewAlterTableEnableRLS(table), nil
}

// parseAlterTableRowSecurity handles an ALTER TABLE tail that begins with
// ENABLE or DISABLE. The second result reports whether the tail was consumed;
// when it was not, the caller falls through to the operation list. Anything
// after ENABLE / DISABLE other than ROW keeps the "unsupported ALTER
// operation" error it had before, so ALTER TABLE ... ENABLE TRIGGER is
// unaffected.
func (p *Parser) parseAlterTableRowSecurity(table string) (ast.Node, bool, error) {
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("ENABLE") && !p.current.MatchIdentifierValue("DISABLE") {
		return nil, false, nil
	}
	keyword := strings.ToUpper(p.current.Value)
	start := p.current.Start
	p.advance()
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("ROW") {
		return nil, true, fmt.Errorf("unsupported ALTER operation: %s at position %d", keyword, start)
	}
	node, err := p.parseAlterTableRowLevelSecurity(table, keyword, start)
	if err != nil {
		return nil, true, err
	}
	return node, true, nil
}

// parseSchemaQualifiedName splits a possibly qualified name into its schema
// prefix and its final part, for the nodes that keep the two apart.
func (p *Parser) parseSchemaQualifiedName(label string) (schema, name string, err error) {
	qualified, err := p.parseQualifiedIdentifier(label)
	if err != nil {
		return "", "", err
	}
	parts := splitQualifiedIdentifier(qualified)
	if len(parts) == 1 {
		return "", parts[0], nil
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1], nil
}

// splitQualifiedIdentifier splits on the dots that separate name parts while
// leaving dots inside a double-quoted part alone. A doubled quote is SQL's
// escape for a literal quote and does not end the quoted part.
func splitQualifiedIdentifier(value string) []string {
	var parts []string
	start := 0
	quoted := false
	for index := 0; index < len(value); index++ {
		switch {
		case value[index] == '"' && quoted && index+1 < len(value) && value[index+1] == '"':
			index++
		case value[index] == '"':
			quoted = !quoted
		case value[index] == '.' && !quoted:
			parts = append(parts, value[start:index])
			start = index + 1
		}
	}
	return append(parts, value[start:])
}

// trimSQLStringLiteral removes the surrounding quotes from a lexed string
// literal and collapses the doubled quotes SQL uses for escaping.
func trimSQLStringLiteral(value string) string {
	if len(value) < 2 {
		return value
	}
	quote := value[0]
	if quote != '\'' && quote != '"' {
		return value
	}
	if value[len(value)-1] != quote {
		return value
	}
	inner := value[1 : len(value)-1]
	return strings.ReplaceAll(inner, string([]byte{quote, quote}), string(quote))
}

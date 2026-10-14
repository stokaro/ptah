package parser

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// This file holds the statement grammar for the PostgreSQL schema objects that
// Ptah's own renderer emits: sequences, roles, grants, default privileges,
// policies, ALTER TABLE ... ENABLE ROW LEVEL SECURITY and materialized views. A
// frontend that refuses them leaves `ptah schema render` unable to read back the
// SQL it has just written (issue #932).

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
	if err := p.parseCreateRoleIfNotExists(); err != nil {
		return nil, err
	}

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

// parseCreateRoleIfNotExists reads the IF NOT EXISTS of a MySQL-family CREATE
// ROLE, which the MySQL-family renderer writes for every role. It changes
// nothing a schema file declares: the role exists after the statement either
// way. Without it, `CREATE ROLE IF NOT EXISTS r` read IF as the role's name,
// so the SQL `schema inspect` wrote for a server holding a role could not be
// read back (stokaro/ptah#3902). PostgreSQL has no such form and answers a
// syntax error, so there the guard is refused by name.
func (p *Parser) parseCreateRoleIfNotExists() error {
	if !p.current.MatchIdentifierValue("IF") {
		return nil
	}
	if p.dialect != "" && !isMySQLFamilyDialect(p.dialect) {
		return fmt.Errorf("CREATE ROLE IF NOT EXISTS at position %d: %s takes no IF NOT EXISTS on CREATE ROLE",
			p.current.Start, p.dialect)
	}
	if _, err := p.parseOptionalIfNotExists(); err != nil {
		return err
	}
	p.skipWhitespace()
	return nil
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
	password, err := p.stringConstant("the password")
	if err != nil {
		return err
	}
	role.SetPassword(password)
	return nil
}

// parseGrantStatement parses GRANT priv[, ...] ON [objtype] name[, ...] TO role[, ...]
// [WITH GRANT OPTION], where a routine target carries its argument types:
// ON FUNCTION name(argtypes).
func (p *Parser) parseGrantStatement() (ast.Node, error) {
	if err := p.expect(lexer.TokenIdentifier, "GRANT"); err != nil {
		return nil, err
	}
	p.skipWhitespace()
	privileges, columns, err := p.parseObjectPrivileges("GRANT")
	if err != nil {
		return nil, err
	}
	targets, err := p.parseGrantTargets()
	if err != nil {
		return nil, err
	}
	if err := requireColumnTarget("GRANT", targets[0], columns); err != nil {
		return nil, err
	}
	if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
		return nil, fmt.Errorf("expected TO after GRANT target: %w", err)
	}
	roles, err := p.parseGrantRoles()
	if err != nil {
		return nil, err
	}
	withOption, err := p.parseGrantOptionSuffix()
	if err != nil {
		return nil, err
	}
	// Expand PostgreSQL's Cartesian product so all consumers keep the same
	// single-object privilege representation.
	grants := &ast.StatementList{}
	for _, target := range targets {
		for _, role := range roles {
			grants.Statements = append(grants.Statements, ast.NewGrantPrivilege(role, target.objectType, target.objectName, privileges).
				SetArguments(target.arguments).SetColumns(columns).SetWithOption(withOption))
		}
	}
	return grants, nil
}

func (p *Parser) parseGrantTargets() ([]grantTarget, error) {
	target, err := p.parseGrantTarget("GRANT")
	if err != nil {
		return nil, err
	}
	targets := []grantTarget{target}
	p.skipWhitespace()
	for p.current.MatchOperatorValue(",") {
		if p.dialect != "" && !platform.IsPostgresFamily(p.dialect) {
			return nil, fmt.Errorf("GRANT target lists are not supported for %s at position %d", p.dialect, p.current.Start)
		}
		p.advance()
		p.skipWhitespace()
		next, err := p.parseGrantObject("GRANT", target.objectType)
		if err != nil {
			return nil, err
		}
		targets = append(targets, next)
		p.skipWhitespace()
	}
	return targets, nil
}

func (p *Parser) parseGrantRoles() ([]string, error) {
	var roles []string
	for {
		p.skipWhitespace()
		role, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected grantee role: %w", err)
		}
		roles = append(roles, role)
		p.skipWhitespace()
		if !p.current.MatchOperatorValue(",") {
			return roles, nil
		}
		p.advance()
	}
}

// parseRevokeStatement parses REVOKE [GRANT OPTION FOR] priv[, ...] ON
// [objtype] name FROM role [RESTRICT].
//
// Three forms PostgreSQL accepts are refused, each because its effect reaches
// past what one statement in a desired-state file can say: CASCADE also revokes
// whatever the role granted onward, GRANTED BY names a grantor the model does
// not key grants by, and a grantee list would be read as its first name.
func (p *Parser) parseRevokeStatement() (*ast.RevokePrivilegeNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "REVOKE"); err != nil {
		return nil, err
	}
	grantOptionFor, err := p.parseRevokeGrantOptionForPrefix()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	privileges, columns, err := p.parseObjectPrivileges("REVOKE")
	if err != nil {
		return nil, err
	}

	target, err := p.parseGrantTarget("REVOKE")
	if err != nil {
		return nil, err
	}
	if err := requireColumnTarget("REVOKE", target, columns); err != nil {
		return nil, err
	}

	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "FROM"); err != nil {
		return nil, fmt.Errorf("expected FROM after REVOKE target: %w", err)
	}
	p.skipWhitespace()
	role, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected grantee role: %w", err)
	}
	if err := p.parseRevokeSuffix(); err != nil {
		return nil, err
	}

	revoke := ast.NewRevokePrivilege(role, target.objectType, target.objectName, privileges).
		SetArguments(target.arguments).
		SetColumns(columns)
	return revoke.SetGrantOptionFor(grantOptionFor), nil
}

// parseRevokeSuffix reads what may follow the grantee of a REVOKE. RESTRICT is
// PostgreSQL's default and changes nothing; see [Parser.parseRevokeStatement]
// for the rest.
func (p *Parser) parseRevokeSuffix() error {
	p.skipWhitespace()
	switch {
	case p.current.MatchOperatorValue(","):
		return fmt.Errorf(
			"REVOKE names one grantee here, not a list, at position %d: write one statement per role", p.current.Start)
	case p.current.MatchIdentifierValue("GRANTED"):
		return fmt.Errorf(
			"REVOKE ... GRANTED BY is not supported at position %d: grants are not keyed by grantor here",
			p.current.Start)
	case p.current.MatchIdentifierValue("CASCADE"):
		return fmt.Errorf(
			"REVOKE ... CASCADE is not supported at position %d: it also revokes what the role granted onward, "+
				"which a desired-state file cannot see", p.current.Start)
	case p.current.MatchIdentifierValue("RESTRICT"):
		p.advance()
	}
	return nil
}

// parseObjectPrivileges reads the privilege list of a GRANT or REVOKE on an
// object, with ON consumed, and the column list its privileges are limited to.
//
// ALL PRIVILEGES names ALL. A privilege may name columns, such as UPDATE
// (a, b). Each privilege must name the same column list because the common
// operation carries one list; a different list is refused.
func (p *Parser) parseObjectPrivileges(statement string) (privileges, columns []string, err error) {
	for {
		p.skipWhitespace()
		privilege, err := p.expectIdentifier()
		if err != nil {
			return nil, nil, fmt.Errorf("expected privilege name: %w", err)
		}
		privilege = strings.ToUpper(privilege)
		p.skipWhitespace()
		if privilege == "ALL" && p.current.MatchIdentifierValue("PRIVILEGES") {
			p.advance()
			p.skipWhitespace()
		}
		start := p.current.Start
		named, err := p.parsePrivilegeColumns(statement)
		if err != nil {
			return nil, nil, err
		}
		if len(privileges) > 0 && !slices.Equal(named, columns) {
			return nil, nil, fmt.Errorf(
				"%s names a different column list for %s at position %d: one statement here limits all of its "+
					"privileges to the same columns, so write one statement per column list", statement, privilege, start)
		}
		columns = named
		privileges = append(privileges, privilege)
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}
		if p.current.MatchIdentifierValue("ON") {
			p.advance()
			return privileges, columns, nil
		}
		return nil, nil, fmt.Errorf("expected ',' or ON in %s privileges at position %d", statement, p.current.Start)
	}
}

// parsePrivilegeColumns reads the column list that may follow a privilege
// name, and returns nil when there is none.
func (p *Parser) parsePrivilegeColumns(statement string) ([]string, error) {
	if !p.current.MatchOperatorValue("(") {
		return nil, nil
	}
	p.advance()
	var columns []string
	for {
		p.skipWhitespace()
		column, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected column name in %s privilege columns: %w", statement, err)
		}
		columns = append(columns, column)
		p.skipWhitespace()
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}
		if p.current.MatchOperatorValue(")") {
			p.advance()
			p.skipWhitespace()
			return columns, nil
		}
		return nil, fmt.Errorf("expected ',' or ')' in %s privilege columns at position %d", statement, p.current.Start)
	}
}

// requireColumnTarget refuses a column list on a target that has no columns.
// PostgreSQL accepts one only on a table, a view or a foreign table, which a
// grant here names as a TABLE.
func requireColumnTarget(statement string, target grantTarget, columns []string) error {
	if len(columns) == 0 || target.objectType == "TABLE" {
		return nil
	}
	return fmt.Errorf("%s names columns on %s %s: column privileges apply to a table", statement,
		target.objectType, target.objectName)
}

// grantObjectTypes are the GRANT target kinds Ptah models. A GRANT that omits
// the keyword entirely targets a table, which is PostgreSQL's own default.
var grantObjectTypes = map[string]bool{
	"TABLE": true, "SCHEMA": true, "SEQUENCE": true,
	"FUNCTION": true, "PROCEDURE": true, "ROUTINE": true,
}

// grantRoutineTypes are the target kinds whose identity includes argument
// types.
var grantRoutineTypes = map[string]bool{"FUNCTION": true, "PROCEDURE": true, "ROUTINE": true}

// grantTarget is the object a GRANT or REVOKE names.
type grantTarget struct {
	objectType string
	objectName string
	arguments  string
}

// parseGrantTarget reads the object after ON.
//
// A routine target must name its argument types. PostgreSQL resolves a bare
// name when exactly one routine carries it, which is a fact about the database
// at the moment the statement runs; a desired-state file is read without one,
// so the grant could not be tied to a routine and is refused instead. The
// ALL ... IN SCHEMA form is refused for the same reason: it grants on whatever
// objects exist when it runs, and a file cannot say which those are.
func (p *Parser) parseGrantTarget(statement string) (grantTarget, error) {
	p.skipWhitespace()
	target := grantTarget{objectType: "TABLE"}
	if p.current.MatchIdentifierValue("ALL") {
		return grantTarget{}, fmt.Errorf(
			"%s ... ON ALL ... IN SCHEMA is not supported at position %d: it applies to the objects that exist "+
				"when it runs, so name each object instead", statement, p.current.Start)
	}
	if p.current.Type == lexer.TokenIdentifier {
		keyword := strings.ToUpper(p.current.Value)
		if grantObjectTypes[keyword] {
			target.objectType = keyword
			p.advance()
			p.skipWhitespace()
		}
	}
	return p.parseGrantObject(statement, target.objectType)
}

func (p *Parser) parseGrantObject(statement, objectType string) (grantTarget, error) {
	target := grantTarget{objectType: objectType}
	objectName, err := p.parseQualifiedIdentifier(statement + " object name")
	if err != nil {
		return grantTarget{}, err
	}
	target.objectName = objectName
	if !grantRoutineTypes[target.objectType] {
		return target, nil
	}
	p.skipWhitespace()
	if !p.current.MatchOperatorValue("(") {
		return grantTarget{}, fmt.Errorf(
			"%s ... ON %s %s needs the argument types at position %d, as in %s(uuid): a routine's identity "+
				"includes them", statement, target.objectType, objectName, p.current.Start, objectName)
	}
	arguments, err := p.collectParenthesizedBody(strings.ToLower(target.objectType) + " arguments")
	if err != nil {
		return grantTarget{}, err
	}
	target.arguments = arguments
	return target, nil
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

// parseRevokeGrantOptionForPrefix reads the optional GRANT OPTION FOR prefix,
// which leaves the privilege and takes away the right to pass it on. No
// PostgreSQL privilege is spelled GRANT, so the keyword at this position can
// only open the prefix.
func (p *Parser) parseRevokeGrantOptionForPrefix() (bool, error) {
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("GRANT") {
		return false, nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "OPTION"); err != nil {
		return false, fmt.Errorf("expected OPTION after REVOKE GRANT: %w", err)
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "FOR"); err != nil {
		return false, fmt.Errorf("expected FOR after REVOKE GRANT OPTION: %w", err)
	}
	return true, nil
}

// parseCreateMaterializedView parses
// CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS <query>, and the owners'
// clauses between the name and AS, such as ClickHouse's REFRESH schedule and
// storage clause.
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
	node := ast.NewCreateMaterializedView(name)
	p.claimOwnerViewClauses(node)
	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return nil, fmt.Errorf("expected AS after materialized view name: %w", err)
	}

	body := p.collectStatementBody()
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("expected materialized view body after AS at position %d", p.current.Start)
	}
	return node.SetBody(body), nil
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

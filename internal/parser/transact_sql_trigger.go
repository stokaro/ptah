package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
	routinebuiltin "ptah.run/internal/routineext/builtin"
	"ptah.run/internal/tableref"
)

// readsTransactSQLTriggers reports whether this read takes CREATE TRIGGER in
// the T-SQL order, as the dialect's descriptor states.
func (p *Parser) readsTransactSQLTriggers() bool {
	return p.target.grammar.Routines.Trigger == descriptor.TriggerTransactSQL
}

// parseTransactSQLTrigger reads a CREATE TRIGGER of the T-SQL trigger grammar,
// from the name after TRIGGER:
//
//	[schema.]name ON table [WITH option [, ...]] {FOR | AFTER | INSTEAD OF}
//	    {INSERT | UPDATE | DELETE} [, ...] [NOT FOR REPLICATION] AS body
//
// FOR is read as AFTER, its synonym, which is how sys.triggers records it, and
// the events are joined by OR in the order written, as the node keeps them. The
// trigger fires once per statement, as every SQL Server DML trigger does, so
// its level is STATEMENT, the level the catalog reader reports. The body runs
// to the end of its batch, and ends where a procedure's does; see
// [routinebuiltin.TransactSQLModuleBody].
//
// What the server refuses is refused here too: IF NOT EXISTS, BEFORE, an
// event list joined by OR, UPDATE OF, TRUNCATE, an event written twice, and
// WITH APPEND. A header clause the server takes and Ptah does not model is
// refused by name rather than read as nothing, because a schema that dropped
// it would compare equal to a trigger without it: ENCRYPTION, an EXECUTE AS
// other than CALLER, NOT FOR REPLICATION, and a DDL or logon trigger.
func (p *Parser) parseTransactSQLTrigger() (ast.Node, error) {
	if p.current.MatchIdentifierValue("IF") {
		return nil, fmt.Errorf("IF after CREATE TRIGGER at position %d: SQL Server takes no IF NOT EXISTS on a trigger; "+
			"write CREATE OR ALTER TRIGGER to replace one that exists", p.current.Start)
	}
	nameParts, err := p.parseIdentifierParts("trigger name")
	if err != nil {
		return nil, err
	}
	written := strings.Join(nameParts, ".")
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
		return nil, fmt.Errorf("expected ON after trigger name %s: %w", written, err)
	}
	p.skipWhitespace()
	if err := p.refuseTransactSQLServerScope(written); err != nil {
		return nil, err
	}

	table, err := p.parseTableName("trigger table name")
	if err != nil {
		return nil, err
	}
	name, err := p.transactSQLTriggerName(nameParts, table)
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if err := p.readTransactSQLTriggerOptions(name); err != nil {
		return nil, err
	}
	timing, err := p.parseTransactSQLTriggerTiming(name)
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	events, err := p.parseTransactSQLTriggerEvents(name)
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	if err := p.refuseTransactSQLTriggerTail(name); err != nil {
		return nil, err
	}
	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return nil, fmt.Errorf("expected AS before the body of trigger %s: %w", name, err)
	}

	body, err := p.collectTransactSQLTriggerBody(name)
	if err != nil {
		return nil, err
	}
	return ast.NewCreateTrigger(name, table).
		SetTiming(timing).
		SetEvent(events).
		SetForEach("STATEMENT").
		SetBody(body), nil
}

// refuseTransactSQLServerScope refuses a trigger that names DATABASE or ALL
// SERVER where a DML trigger names its table: a DDL trigger, or a logon
// trigger. Ptah models DML triggers alone, and the catalog reader reads the
// triggers of tables alone, so such a trigger could be neither compared nor
// planned. DATABASE and ALL are reserved words in T-SQL, so no table is
// written that way.
//
// The returned error satisfies errors.Is(err, [ptaherr.ErrUnsupportedFeature]).
func (p *Parser) refuseTransactSQLServerScope(name string) error {
	var scope string
	switch {
	case p.current.MatchIdentifierValue("DATABASE"):
		scope = "DATABASE"
	case p.current.MatchIdentifierValue("ALL"):
		scope = "ALL SERVER"
	default:
		return nil
	}
	return &ptaherr.CapabilityError{
		Dialect: platform.NormalizeDialect(p.dialect),
		Feature: "SQL Server DDL and logon triggers",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("CREATE TRIGGER %s ON %s at position %d is a DDL or logon trigger, which Ptah does not model; "+
			"a schema declares DML triggers on tables alone, so keep this trigger out of the schema",
			name, scope, p.current.Start),
	}
}

// transactSQLTriggerName returns the trigger's name without a schema. A DML
// trigger lives in the schema of its table, and SQL Server refuses a name
// that states another one with Msg 2103, so a schema written in the name says
// nothing the table does not. One that differs from the table's is refused as
// the server refuses it; an unqualified table is in the dialect's default
// schema. Names are compared without case, as SQL Server's default collation
// compares them. A name with a database prefix is refused too, as the server
// refuses it with Msg 166.
func (p *Parser) transactSQLTriggerName(nameParts []string, table string) (string, error) {
	written := strings.Join(nameParts, ".")
	switch len(nameParts) {
	case 1:
		return written, nil
	case 2:
	default:
		return "", fmt.Errorf("trigger name %s has a database prefix, which SQL Server refuses with Msg 166; "+
			"write [schema.]name", written)
	}
	triggerSchema := unquotedNamePart(nameParts[0])
	tableSchema := p.target.defaultSchema
	if ref, ok := tableref.Parse(table); ok && ref.Qualified {
		tableSchema = ref.Schema
	}
	if tableSchema != "" && !strings.EqualFold(triggerSchema, tableSchema) {
		return "", fmt.Errorf("trigger %s is in schema %s and its table %s in schema %s; "+
			"SQL Server refuses a trigger outside its table's schema with Msg 2103", written, triggerSchema, table, tableSchema)
	}
	return nameParts[1], nil
}

// unquotedNamePart is one part of a name without its quotes or brackets.
func unquotedNamePart(part string) string {
	if ref, ok := tableref.Parse(part); ok && !ref.Qualified {
		return ref.Name
	}
	return part
}

// readTransactSQLTriggerOptions reads the WITH list between a trigger's table
// and its timing. EXECUTE AS CALLER is the default, and sys.sql_modules
// records it as it records a trigger that writes no clause, so it is read as
// nothing. Every other option is refused by name; see
// [Parser.refuseTransactSQLTriggerOption].
func (p *Parser) readTransactSQLTriggerOptions(name string) error {
	if !p.current.MatchIdentifierValue("WITH") {
		return nil
	}
	p.advance()
	p.skipWhitespace()
	for {
		start := p.current.Start
		option := p.readTransactSQLTriggerOption()
		if option == "" {
			return fmt.Errorf("expected a trigger option at position %d", start)
		}
		if err := p.refuseTransactSQLTriggerOption(name, option, start); err != nil {
			return err
		}
		if !p.current.MatchOperatorValue(",") {
			return nil
		}
		p.advance()
		p.skipWhitespace()
	}
}

// readTransactSQLTriggerOption reads one option of a WITH list, which ends at
// a comma, at the timing that follows the list, or at the end of the
// statement. Its words are in upper case and a string keeps its quotes, so an
// option has one spelling: `EXECUTE AS 'auditor'`.
func (p *Parser) readTransactSQLTriggerOption() string {
	var words []string
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon && !p.current.MatchOperatorValue(",") &&
		!p.current.MatchIdentifierValue("FOR") && !p.current.MatchIdentifierValue("AFTER") &&
		!p.current.MatchIdentifierValue("INSTEAD") {
		switch p.current.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
		case lexer.TokenString:
			words = append(words, p.current.Value)
		default:
			words = append(words, strings.ToUpper(p.current.Value))
		}
		p.advance()
	}
	return strings.Join(words, " ")
}

// refuseTransactSQLTriggerOption accepts EXECUTE AS CALLER and refuses every
// other trigger option by name, with the reason it cannot be kept:
//
//   - ENCRYPTION hides the trigger's definition. Measured on SQL Server 2025,
//     OBJECT_DEFINITION is NULL for an encrypted trigger, so its body could
//     never be compared, nor written back by a plan.
//   - Any other EXECUTE AS names a principal, which sys.sql_modules records
//     (-2 for OWNER, the creating user's id for SELF) and the schema model has
//     no place for.
//   - NATIVE_COMPILATION and SCHEMABINDING belong to a trigger on a
//     memory-optimized table, which Ptah does not model either.
//
// The returned error satisfies errors.Is(err, [ptaherr.ErrUnsupportedFeature]).
func (p *Parser) refuseTransactSQLTriggerOption(name, option string, position int) error {
	var reason string
	switch {
	case option == "EXECUTE AS CALLER":
		return nil
	case option == "ENCRYPTION":
		reason = "SQL Server keeps no readable definition of an encrypted trigger, so Ptah could neither compare its body nor write it back"
	case strings.HasPrefix(option, "EXECUTE AS "):
		reason = "Ptah does not model the principal a trigger runs as, so it could neither write it nor compare it back"
	default:
		reason = "Ptah can neither write the option nor compare it back"
	}
	return &ptaherr.CapabilityError{
		Dialect: platform.NormalizeDialect(p.dialect),
		Feature: "SQL Server trigger options",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("trigger %s states WITH %s at position %d: %s; remove it, or keep the trigger out of the schema",
			name, option, position, reason),
	}
}

// parseTransactSQLTriggerTiming reads FOR, AFTER or INSTEAD OF. FOR and AFTER
// are one timing, which the catalog reports as AFTER.
func (p *Parser) parseTransactSQLTriggerTiming(name string) (string, error) {
	switch {
	case p.current.MatchIdentifierValue("FOR"), p.current.MatchIdentifierValue("AFTER"):
		p.advance()
		return "AFTER", nil
	case p.current.MatchIdentifierValue("INSTEAD"):
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "OF"); err != nil {
			return "", fmt.Errorf("expected OF after INSTEAD in trigger %s: %w", name, err)
		}
		return "INSTEAD OF", nil
	default:
		return "", fmt.Errorf("expected FOR, AFTER or INSTEAD OF in trigger %s, got %s %q at position %d",
			name, p.current.Type, p.current.Value, p.current.Start)
	}
}

// parseTransactSQLTriggerEvents reads the comma-separated events a trigger
// fires on, each of INSERT, UPDATE and DELETE at most once, and joins them by
// OR in the order written.
func (p *Parser) parseTransactSQLTriggerEvents(name string) (string, error) {
	var events []string
	for {
		if !p.current.MatchIdentifierValue("INSERT") && !p.current.MatchIdentifierValue("UPDATE") &&
			!p.current.MatchIdentifierValue("DELETE") {
			return "", fmt.Errorf("expected INSERT, UPDATE or DELETE as an event of trigger %s, got %s %q at position %d",
				name, p.current.Type, p.current.Value, p.current.Start)
		}
		event := strings.ToUpper(p.current.Value)
		if slices.Contains(events, event) {
			return "", fmt.Errorf("trigger %s names the event %s twice at position %d, which SQL Server refuses with Msg 1034",
				name, event, p.current.Start)
		}
		events = append(events, event)
		p.advance()
		p.skipWhitespace()
		if !p.current.MatchOperatorValue(",") {
			return strings.Join(events, " OR "), nil
		}
		p.advance()
		p.skipWhitespace()
	}
}

// refuseTransactSQLTriggerTail refuses the clauses T-SQL writes between a
// trigger's events and its AS. WITH APPEND is refused as SQL Server 2025
// refuses it, with Msg 195, 'APPEND' is not a recognized option. NOT FOR
// REPLICATION is refused because sys.triggers records it in
// is_not_for_replication and the schema model has no place for it, so it
// could be neither written nor compared back.
//
// The NOT FOR REPLICATION refusal satisfies errors.Is(err,
// [ptaherr.ErrUnsupportedFeature]).
func (p *Parser) refuseTransactSQLTriggerTail(name string) error {
	start := p.current.Start
	switch {
	case p.current.MatchIdentifierValue("WITH"):
		p.advance()
		p.skipWhitespace()
		if p.current.MatchIdentifierValue("APPEND") {
			return fmt.Errorf("trigger %s states WITH APPEND at position %d, which SQL Server 2025 refuses with Msg 195, "+
				"'APPEND' is not a recognized option; remove it", name, start)
		}
		return fmt.Errorf("expected APPEND after WITH in trigger %s at position %d", name, p.current.Start)
	case p.current.MatchIdentifierValue("NOT"):
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "FOR"); err != nil {
			return fmt.Errorf("expected FOR REPLICATION after NOT in trigger %s: %w", name, err)
		}
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "REPLICATION"); err != nil {
			return fmt.Errorf("expected REPLICATION after NOT FOR in trigger %s: %w", name, err)
		}
		return &ptaherr.CapabilityError{
			Dialect: platform.NormalizeDialect(p.dialect),
			Feature: "SQL Server trigger options",
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("trigger %s states NOT FOR REPLICATION at position %d: Ptah does not model whether "+
				"replication fires a trigger, so it could neither write the setting nor compare it back; "+
				"remove it, or keep the trigger out of the schema", name, start),
		}
	default:
		return nil
	}
}

// collectTransactSQLTriggerBody reads the body after AS. A semicolon outside a
// block is a line of the body, as it is to the server, which ends the body
// with the batch; the reader ends it where it ends a procedure's, at the END
// closing its last open block, at a GO batch separator or at the end of the
// input. The SQL Server script splitter cuts the forms `ptah db read` prints,
// a block body followed by a semicolon, and a body followed by GO at the same
// places.
func (p *Parser) collectTransactSQLTriggerBody(name string) (string, error) {
	p.skipWhitespace()
	if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon || p.isCurrentSQLServerGoBatchSeparator() {
		return "", fmt.Errorf("expected the body of trigger %s after AS at position %d", name, p.current.Start)
	}
	return routinebuiltin.TransactSQLModuleBody(routineCursor{p}, p.current.Start)
}

package migrator

import (
	"context"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/lexer"
)

type postgresIndexConstraintAttachment struct {
	Index postgresIndexRef
	Name  string
}

// PostgreSQL renames an index to the explicit constraint name when attaching
// it with ADD CONSTRAINT ... USING INDEX. Both execution and committed-prefix
// reconstruction must follow that name before checking the catalog.
func (m *Migrator) observePostgresIndexConstraintAttachments(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statement string,
	searchPathKnowledge postgresSearchPathKnowledge,
) error {
	for _, attachment := range postgresIndexConstraintAttachments(statement) {
		if attachment.Index.Schema == "" && searchPathKnowledge == postgresSearchPathKnown {
			var target postgresIndexState
			if err := m.resolvePostgresIndexTarget(ctx, conn, attachment.Index, &target); err != nil {
				return err
			}
			if !target.TargetFound {
				continue
			}
			attachment.Index.Schema = target.TargetSchema
		}
		m.postgresIndexObservation.attachConstraint(attachment)
	}
	return nil
}

func (o *postgresIndexObservation) attachConstraint(attachment postgresIndexConstraintAttachment) {
	for i := range o.identities {
		identity := &o.identities[i]
		if identity.TargetTable == attachment.Index.Table && identity.Name == attachment.Index.Name &&
			(attachment.Index.Schema == "" || identity.TargetSchema == attachment.Index.Schema) {
			identity.Name = attachment.Name
		}
	}
}

func postgresIndexConstraintAttachments(statement string) []postgresIndexConstraintAttachment {
	tokens := significantSQLTokens(statement, platform.Postgres)
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("ALTER") || !tokens[1].MatchIdentifierValue("TABLE") {
		return nil
	}
	tokens = tokens[2:]
	if len(tokens) >= 2 && tokens[0].MatchIdentifierValue("IF") && tokens[1].MatchIdentifierValue("EXISTS") {
		tokens = tokens[2:]
	}
	tokens = skipKeywordToken(tokens, "ONLY")
	target, tail, ok := postgresAttachmentTarget(tokens)
	if !ok {
		return nil
	}
	var attachments []postgresIndexConstraintAttachment
	depth := 0
	actionStart := true
	for i, token := range tail {
		if token.MatchOperatorValue(";") {
			break
		}
		if actionStart {
			if attachment, ok := parsePostgresIndexConstraintAttachment(target, tail[i:]); ok {
				attachments = append(attachments, attachment)
			}
			actionStart = false
		}
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case token.MatchOperatorValue(",") && depth == 0:
			actionStart = true
		}
	}
	return attachments
}

func postgresAttachmentTarget(tokens []lexer.Token) (postgresIndexRef, []lexer.Token, bool) {
	if len(tokens) == 0 {
		return postgresIndexRef{}, nil, false
	}
	name, ok := postgresIdentifierValue(tokens[0])
	if !ok {
		return postgresIndexRef{}, nil, false
	}
	target := postgresIndexRef{Table: name}
	tokens = tokens[1:]
	if len(tokens) >= 2 && tokens[0].MatchOperatorValue(".") {
		table, ok := postgresIdentifierValue(tokens[1])
		if !ok {
			return postgresIndexRef{}, nil, false
		}
		target.Schema, target.Table = name, table
		tokens = tokens[2:]
	}
	if len(tokens) > 0 && tokens[0].MatchOperatorValue("*") {
		tokens = tokens[1:]
	}
	return target, tokens, true
}

func parsePostgresIndexConstraintAttachment(
	target postgresIndexRef,
	tokens []lexer.Token,
) (postgresIndexConstraintAttachment, bool) {
	if len(tokens) < 7 || !tokens[0].MatchIdentifierValue("ADD") || !tokens[1].MatchIdentifierValue("CONSTRAINT") {
		return postgresIndexConstraintAttachment{}, false
	}
	name, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return postgresIndexConstraintAttachment{}, false
	}
	tokens = tokens[3:]
	switch {
	case tokens[0].MatchIdentifierValue("UNIQUE"):
		tokens = tokens[1:]
	case tokens[0].MatchIdentifierValue("PRIMARY") && tokens[1].MatchIdentifierValue("KEY"):
		tokens = tokens[2:]
	default:
		return postgresIndexConstraintAttachment{}, false
	}
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("USING") || !tokens[1].MatchIdentifierValue("INDEX") {
		return postgresIndexConstraintAttachment{}, false
	}
	index, ok := postgresIdentifierValue(tokens[2])
	if !ok {
		return postgresIndexConstraintAttachment{}, false
	}
	target.Name = index
	return postgresIndexConstraintAttachment{Index: target, Name: name}, true
}

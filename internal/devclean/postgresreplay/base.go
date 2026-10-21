package postgresreplay

import (
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func (p protection) validateBase(tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	dialect := p.dialect
	first := replay.NormalizedIdentifier(tokens[0])
	if first == "COPY" && isExternalPostgresCopy(tokens) {
		return replay.Refuse(dialect, "external COPY")
	}
	if first == "ALTER" && replay.TokenSequenceAt(tokens, 1, "SYSTEM") {
		return replay.Refuse(dialect, "ALTER SYSTEM")
	}
	if first == "SET" && replay.TokenSequenceAt(tokens, 1, "ROLE") {
		return replay.Refuse(dialect, "SET ROLE")
	}
	if first == "SET" && replay.TokenSequenceAt(tokens, 1, "SESSION", "AUTHORIZATION") {
		return replay.Refuse(dialect, "SET SESSION AUTHORIZATION")
	}
	if isGlobalPostgresDDL(tokens) {
		return replay.Refuse(dialect, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) && (!p.sessionTables || !createsTemporaryTable(tokens)) {
		return replay.Refuse(dialect, "TEMP object")
	}
	return nil
}

func createsTemporaryTable(tokens []lexer.Token) bool {
	if !replay.TokenSequenceAt(tokens, 0, "CREATE") {
		return false
	}
	kind := replay.StatementObjectKindIndex(tokens)
	return kind != replay.MutationTargetNotFound && replay.NormalizedIdentifier(tokens[kind]) == "TABLE"
}

func isGlobalPostgresDDL(tokens []lexer.Token) bool {
	if !replay.IsDDLAction(tokens) {
		return false
	}
	// ALTER DEFAULT PRIVILEGES FOR ROLE names a role without altering it: the
	// statement sets pg_default_acl rows of this database, which the realm
	// cleanup revokes or resets. Read for its object kind, it is ALTER ROLE.
	if replay.TokenSequenceAt(tokens, 0, "ALTER", "DEFAULT", "PRIVILEGES") {
		return false
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "ROLE", "USER", "TABLESPACE", "SUBSCRIPTION",
		"PUBLICATION", "CAST", "LANGUAGE", "TRANSFORM":
		return true
	case "EVENT":
		return replay.TokenSequenceAt(tokens, kindIndex, "EVENT", "TRIGGER")
	case "FOREIGN":
		return replay.NormalizedIdentifier(tokens[0]) != "DROP" ||
			!replay.TokenSequenceAt(tokens, kindIndex, "FOREIGN", "TABLE")
	case "SERVER":
		return true
	case "TEXT":
		return replay.TokenSequenceAt(tokens, kindIndex, "TEXT", "SEARCH")
	case "ACCESS":
		return replay.TokenSequenceAt(tokens, kindIndex, "ACCESS", "METHOD")
	default:
		return false
	}
}

func isExternalPostgresCopy(tokens []lexer.Token) bool {
	if replay.ContainsIdentifier(tokens, "PROGRAM") {
		return true
	}
	toIndex := replay.FindIdentifier(tokens, "TO", 1)
	if toIndex == replay.MutationTargetNotFound || toIndex+1 >= len(tokens) {
		return false
	}
	return tokens[toIndex+1].Type == lexer.TokenString &&
		replay.NormalizedIdentifier(tokens[toIndex+1]) != "STDOUT"
}

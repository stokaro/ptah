package postgresreplay

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/sqlsyntax/pgsyntax"
)

func (p protection) validateBase(tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	dialect := p.dialect
	first := replay.NormalizedIdentifier(tokens[0])
	if first == "COPY" && !p.trustedSQL && isExternalPostgresCopy(tokens) {
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
		if !p.confinedPublicationDDL(tokens) {
			return replay.Refuse(dialect, replay.DescribeStatementObject(tokens))
		}
		if kept := p.keptPublication(tokens); kept != "" {
			return replay.Refuse(dialect, fmt.Sprintf(
				"%s of the dev database's own publication %q", replay.DescribeStatementObject(tokens), kept))
		}
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

// confinedPublicationDDL reports CREATE, ALTER or DROP PUBLICATION on
// PostgreSQL where every cleanup of the run drops the publications its claim
// did not find. A replay may then run one although a publication belongs to
// the whole database, since a publication the run creates does not outlive
// the run; a desired schema that declares a publication is rehearsed this way.
// A change to or the drop of a publication the claim found would outlive it,
// so [protection.keptPublication] refuses those. Without such a cleanup, as on
// a claim that could not list the database's objects, every publication
// statement is refused. No other engine of the family is lifted: Ptah models
// no publication there, and a YugabyteDB cleanup is not asked to drop one.
func (p protection) confinedPublicationDDL(tokens []lexer.Token) bool {
	if p.dialect != platform.Postgres || !p.publications.Dropped {
		return false
	}
	kind := replay.StatementObjectKindIndex(tokens)
	return kind != replay.MutationTargetNotFound && replay.NormalizedIdentifier(tokens[kind]) == "PUBLICATION"
}

// keptPublication names the publication the claim found that an ALTER or DROP
// PUBLICATION changes, or is "". The cleanup keeps such a publication as it
// finds it, so an ADD TABLE, a SET, an OWNER TO or a drop the run made to it
// would stay after the run; one the run created is dropped whatever it did to
// it. A name is compared as the server stores it: folded to lower case unless
// it was quoted.
func (p protection) keptPublication(tokens []lexer.Token) string {
	if len(p.publications.Kept) == 0 || replay.TokenSequenceAt(tokens, 0, "CREATE") {
		return ""
	}
	kind := replay.StatementObjectKindIndex(tokens)
	for index := replay.ObjectNameIndex(tokens, kind); index < len(tokens); index++ {
		token := tokens[index]
		if token.Type != lexer.TokenIdentifier && !quotedIdentifier(token) {
			continue
		}
		if name := storedName(token); slices.Contains(p.publications.Kept, name) {
			return name
		}
		if !replay.TokenSequenceAt(tokens, 0, "DROP") {
			return ""
		}
	}
	return ""
}

// quotedIdentifier reports a double-quoted identifier, which the lexer reads
// as a string token.
func quotedIdentifier(token lexer.Token) bool {
	return token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`)
}

// decodedUnicodeIdentifiers returns tokens with each U&"..." identifier read
// as the quoted identifier it spells, its UESCAPE clause included. The lexer
// reads one as the word U, an operator and a string it does not decode, so
// without this `UPDATE U&"\0070g_catalog".pg_settings` names a schema U to
// every check, while the server decodes the escape before it looks a name up
// and writes to pg_catalog. Each is decoded as the server decodes it
// (pgsyntax.Value) from the three adjacent tokens the server reads as one:
// with a space between them, `u & "mask"` is an operator over two columns.
//
// It reports false for an identifier the server does not decode either, an
// escape it refuses or an empty name, so the guard refuses the statement
// rather than read a name the server would not.
func decodedUnicodeIdentifiers(stmt string, tokens []lexer.Token) ([]lexer.Token, bool) {
	decoded := make([]lexer.Token, 0, len(tokens))
	for index := 0; index < len(tokens); index++ {
		if !unicodeIdentifierAt(tokens, index) {
			decoded = append(decoded, tokens[index])
			continue
		}
		last := index + 2
		if last+2 < len(tokens) && replay.TokenSequenceAt(tokens, last+1, "UESCAPE") {
			last += 2
		}
		text := stmt[tokens[index].Start:tokens[last].End]
		scanned, err := pgsyntax.Scan(text, pgsyntax.Options{})
		if err != nil {
			return nil, false
		}
		name, next, ok := pgsyntax.Value(text, scanned, 0, pgsyntax.Options{})
		if !ok || next != len(scanned) {
			return nil, false
		}
		decoded = append(decoded, lexer.Token{
			Type:  lexer.TokenString,
			Value: pgsyntax.QuoteIdentifier(name),
			Start: tokens[index].Start,
			End:   tokens[last].End,
		})
		index = last
	}
	return decoded, true
}

// unicodeIdentifierAt reports the word U, an ampersand and a quoted identifier
// written with nothing between them, which the server reads as one U&"..."
// identifier.
func unicodeIdentifierAt(tokens []lexer.Token, index int) bool {
	if index+2 >= len(tokens) {
		return false
	}
	introducer, ampersand, quoted := tokens[index], tokens[index+1], tokens[index+2]
	return introducer.Type == lexer.TokenIdentifier && strings.EqualFold(introducer.Value, "U") &&
		ampersand.MatchOperatorValue("&") && quotedIdentifier(quoted) &&
		introducer.End == ampersand.Start && ampersand.End == quoted.Start
}

// storedName is token as PostgreSQL stores the name it spells. An unquoted
// name is folded to lower case in ASCII only, as the server folds it: Étoile
// stays Étoile, where a Unicode fold would compare étoile and miss the
// publication, the extension or the schema the claim found.
func storedName(token lexer.Token) string {
	if quotedIdentifier(token) {
		return replay.IdentifierValue(token)
	}
	return strings.Map(func(r rune) rune {
		if 'A' <= r && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, token.Value)
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

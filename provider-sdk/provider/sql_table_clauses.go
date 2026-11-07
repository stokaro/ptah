package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// SQLTableClausesFeature enables SQLSchemaDeclaration.table_clauses and
// SourceFragment.sql_table_clause: the clauses of CREATE TABLE an engine
// whose SQL documents the host reads with the shared grammar (the FRAGMENTS
// mode) reads itself, and the fragment that carries one such clause to
// DecodeSource. In a SQL document of the engine the host captures the clause
// that opens with a claim's words among the options after the column list,
// up to the next top-level comma or the end of the statement, binds it to its
// table, and sends its text whole. The engine returns the facets of the table
// the clause declares. A SQL document of the engine states each claimed model
// in full for every table it declares, so a table without the clause has
// none. It is a two-way feature. A provider lists it, and so does each engine
// that claims a clause, beside DecodeSourceFeature. Contract 25.29.
const SQLTableClausesFeature = "source.sql-table-clauses"

// validateSQLTableClauses checks an engine's table-clause claims: present
// exactly when the engine lists the feature, which needs the provider's and
// source decoding and an engine that reads SQL in FRAGMENTS mode; each claim
// with its words, a selector and models; no two claims opening with one word;
// and each model a desired model of the engine that SQL documents state.
func validateSQLTableClauses(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), SQLTableClausesFeature)
	claims := engine.GetSqlSchema().GetTableClauses()
	if listed && (!slices.Contains(description.GetFeatures(), SQLTableClausesFeature) || !slices.Contains(description.GetFeatures(), DecodeSourceFeature)) {
		return sqlSchemaError("table-clause claims need the provider's " + SQLTableClausesFeature + " and source decoding")
	}
	if listed != (len(claims) != 0) {
		return sqlSchemaError("an engine claims table clauses exactly when it lists " + SQLTableClausesFeature)
	}
	if len(claims) != 0 && engine.GetSqlSchema().GetMode() != protocol.SQLSchemaDeclaration_FRAGMENTS {
		return sqlSchemaError("table-clause claims need the FRAGMENTS mode")
	}
	opening := make(map[string]bool)
	for _, claim := range claims {
		if len(claim.GetWords()) == 0 || strings.TrimSpace(claim.GetSelector()) == "" || len(claim.GetModels()) == 0 {
			return sqlSchemaError("incomplete table-clause claim")
		}
		for _, word := range claim.GetWords() {
			if !clauseWord(word) {
				return sqlSchemaError("a table-clause word is not one upper-case word")
			}
		}
		if opening[claim.GetWords()[0]] {
			return sqlSchemaError("two table-clause claims open with one word")
		}
		opening[claim.GetWords()[0]] = true
		for i, kind := range claim.GetModels() {
			model := findValueModel(description, engine.GetId(), kind, protocol.ValueInput_DESIRED)
			if model == nil || !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_SQL) || slices.Contains(claim.GetModels()[:i], kind) {
				return sqlSchemaError("a table-clause claim names an unknown, duplicate or non-SQL desired model")
			}
		}
	}
	return nil
}

// clauseWord reports whether word is one word of ASCII upper-case letters,
// digits and underscores that does not start with a digit.
func clauseWord(word string) bool {
	if word == "" || word[0] >= '0' && word[0] <= '9' {
		return false
	}
	for _, r := range word {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// negotiateSQLTableClauses removes the table-clause claims unless the host
// lists SQLTableClausesFeature.
func negotiateSQLTableClauses(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, SQLTableClausesFeature) && engine.GetSqlSchema() != nil {
		engine.SqlSchema.TableClauses = nil
	}
}

// SQLTableClauseClaim returns the claim of engine that a clause's text opens
// with, or nil when it opens with the words of none. The words are matched
// without regard to case, and each must be followed in the text by white space,
// punctuation or the end of the text.
func SQLTableClauseClaim(engine *protocol.Engine, text string) *protocol.SQLTableClause {
	for _, claim := range engine.GetSqlSchema().GetTableClauses() {
		if opensWith(text, claim.GetWords()) {
			return claim
		}
	}
	return nil
}

// opensWith reports whether text opens with words, separated by white space.
func opensWith(text string, words []string) bool {
	rest := text
	for _, word := range words {
		rest = strings.TrimLeft(rest, " \t\r\n\f\v")
		if len(rest) < len(word) || !strings.EqualFold(rest[:len(word)], word) {
			return false
		}
		rest = rest[len(word):]
		if rest != "" && wordByte(rest[0]) {
			return false
		}
	}
	return true
}

// wordByte reports whether b continues a clause word.
func wordByte(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_'
}

// validateSQLTableClauseFragments binds the table-clause fragments of a
// document to the engine's negotiated claims: a SQL document, the claim and
// its selector unchanged, its models in the document's scope, a table binding
// that is also the subject, and content that opens with the claim's words.
func validateSQLTableClauseFragments(description *protocol.Description, engine string, document *protocol.SourceDocument, models map[string]bool) error {
	var declaring *protocol.Engine
	for _, declared := range description.GetEngines() {
		if declared.GetId() == engine {
			declaring = declared
		}
	}
	for _, fragment := range document.GetFragments() {
		claim := fragment.GetSqlTableClause()
		if claim == nil {
			continue
		}
		if document.GetFormat() != protocol.SourceDocument_SQL {
			return sourceError("a table-clause fragment outside a SQL document")
		}
		if !slices.ContainsFunc(declaring.GetSqlSchema().GetTableClauses(), func(declared *protocol.SQLTableClause) bool { return proto.Equal(claim, declared) }) ||
			fragment.GetSelector() != claim.GetSelector() {
			return sourceError("table-clause fragment changed its negotiated claim")
		}
		for _, kind := range claim.GetModels() {
			if !models[kind] {
				return sourceError("table-clause fragment model is outside document scope")
			}
		}
		table := indexKindTable(document.GetBindings(), fragment)
		if table == nil || !proto.Equal(fragment.GetSubject(), table) {
			return sourceError("table-clause fragment needs its table's binding and that table as its subject")
		}
		if !opensWith(fragment.GetSql(), claim.GetWords()) {
			return sourceError("table-clause fragment does not open with its claim's words")
		}
	}
	return nil
}

// validateSQLTableClauseValue requires a value decoded from a table-clause
// fragment to be a facet of the fragment's table, of a model of its claim.
func validateSQLTableClauseValue(document *protocol.SourceDocument, fragment *protocol.SourceFragment, value *protocol.SourceModelValue) error {
	claim := fragment.GetSqlTableClause()
	if claim == nil {
		return nil
	}
	envelope := value.GetValue().GetValue()
	if !slices.Contains(claim.GetModels(), envelope.GetKind()) {
		return sourceError("decoded value is outside its table-clause claim")
	}
	if value.GetPlacement() != protocol.SourceModelValue_FACET || !sameTable(envelope.GetOwner(), indexKindTable(document.GetBindings(), fragment)) {
		return sourceError("a value of a table-clause fragment is a facet of the clause's table")
	}
	return nil
}

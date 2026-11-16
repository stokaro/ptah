package policyprobe

import (
	"ptah.run/core/objectidentity"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ownqualifier"
)

// ownSchemaQualifiers returns the index of each token that is the schema of
// the policy's own table, written in front of that table and a column: the
// `public` of `public.notes.owner` in a policy on public.notes. The probe and
// the spelling comparison both take it out; see package ownqualifier.
func ownSchemaQualifiers(tokens []lexer.Token, ref objectidentity.ID) map[int]bool {
	schema, table := refMatchers(ref)
	found := make(map[int]bool)
	for _, i := range ownqualifier.Qualifiers(tokens, schema, table) {
		found[i] = true
	}
	return found
}

// probeClause is clause with each of [ownSchemaQualifiers] taken out, and the
// rest byte for byte as declared. A nil clause stays nil.
func probeClause(clause *string, ref objectidentity.ID) *string {
	if clause == nil {
		return nil
	}
	schema, table := refMatchers(ref)
	return new(ownqualifier.Strip(*clause, schema, table))
}

// refMatchers match the schema and the table of a policy reference, quoted or
// not.
func refMatchers(ref objectidentity.ID) (schema, table ownqualifier.Matcher) {
	return func(token lexer.Token) bool { return namesPart(token, ref.Schema) },
		func(token lexer.Token) bool { return namesPart(token, ref.Parent) }
}

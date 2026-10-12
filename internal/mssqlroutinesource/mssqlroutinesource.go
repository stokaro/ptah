// Package mssqlroutinesource is the SQL Server owner's routine syntax in the
// SQL frontend: the options of a routine's WITH list the owner's facet holds
// (see [mssqlschema.RoutineOptionsKind]), the principal its EXECUTE AS names
// among them. The frontend reads the rest of a routine itself, and refuses an
// option no owner reads.
package mssqlroutinesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// The owner's SQL patterns, which each claim carries back.
const (
	executeAs         = "execute-as"
	schemaBinding     = "schemabinding"
	recompile         = "recompile"
	returnsNullOnNull = "returns-null-on-null-input"
	calledOnNull      = "called-on-null-input"
)

// SQL is the owner's contribution to the SQL frontend. It reads the options of
// the WITH list of a CREATE FUNCTION or CREATE PROCEDURE of a SQL Server
// document that the facet holds: SCHEMABINDING, RECOMPILE, RETURNS NULL ON
// NULL INPUT and EXECUTE AS. `EXECUTE AS 'auditor'` and `EXECUTE AS
// N'auditor'` give the routine the principal; CALLER, OWNER and SELF give it
// none, and the frontend reads them as the routine's security mode or refuses
// them. The options of one routine become one facet.
//
// Its claim is [mssqlschema.RoutineOptionsCoverage]: a SQL Server document
// states a routine's options in its CREATE statement, so a routine without
// them holds none.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   mssqlschema.RoutineOwner,
		Targets: []string{platform.SQLServer},
		Patterns: []sqlext.Pattern{
			{Name: executeAs, Site: sqlext.RoutineOption, Words: []string{"EXECUTE"}, Next: []string{"AS"}, Targeted: true},
			{Name: schemaBinding, Site: sqlext.RoutineOption, Words: []string{"SCHEMABINDING"}, Targeted: true},
			{Name: recompile, Site: sqlext.RoutineOption, Words: []string{"RECOMPILE"}, Targeted: true},
			{Name: calledOnNull, Site: sqlext.RoutineOption, Words: []string{"CALLED"}, Next: []string{"ON"}, Targeted: true},
			{Name: returnsNullOnNull, Site: sqlext.RoutineOption, Words: []string{"RETURNS"}, Next: []string{"NULL"}, Targeted: true},
		},
		Facets:   []schemaext.Kind{mssqlschema.RoutineOptionsKind},
		Parser:   parser{},
		Coverage: sqlCoverage,
	}
}

// sqlCoverage is the owner's claim for a SQL Server document. The owner reads
// no header entry, so it is handed none.
func sqlCoverage(sqlext.CoverageRequest) (schemaext.Coverage, error) {
	return mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// parser reads the spans the owner's patterns claim.
type parser struct{}

// ParseSQL reads every claim of one document. The claims of one routine
// share a definition number; their options become one facet, answered on the
// routine's first claim, and the others answer an empty value. An option
// written twice for one routine is refused.
func (parser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, len(request.Claims))
	first := make(map[int]int)
	options := make(map[int]mssqlschema.RoutineOptions)
	var order []int
	seen := make(map[optionKey]bool)
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if _, seen := first[claim.Definition]; !seen {
			first[claim.Definition] = i
			order = append(order, claim.Definition)
		}
		key := optionKey{definition: claim.Definition, pattern: claim.Pattern}
		if key.pattern == calledOnNull {
			key.pattern = returnsNullOnNull
		}
		if seen[key] {
			return sqlext.Result{}, repeatedOption(claim)
		}
		seen[key] = true
		merged, err := readOption(claim, options[claim.Definition])
		if err != nil {
			return sqlext.Result{}, err
		}
		options[claim.Definition] = merged
	}
	for _, definition := range order {
		if options[definition].IsZero() {
			continue
		}
		facets, err := mssqlschema.WithRoutineOptions(schemaext.Facets{}, options[definition])
		if err != nil {
			return sqlext.Result{}, refusal(request.Claims[first[definition]], err.Error())
		}
		values[first[definition]] = sqlext.Value{Facets: facets}
	}
	return sqlext.Result{Values: values}, nil
}

// optionKey identifies an option within one routine header. Empty option values
// still occupy their slot, so EXECUTE AS CALLER cannot hide a duplicate.
type optionKey struct {
	definition int
	pattern    string
}

func repeatedOption(claim sqlext.Claim) error {
	if claim.Pattern == executeAs {
		return refusal(claim, "the routine states EXECUTE AS twice")
	}
	return refusal(claim, "the routine states the option twice")
}

// readOption adds the option claim states to the routine's options so far.
func readOption(claim sqlext.Claim, options mssqlschema.RoutineOptions) (mssqlschema.RoutineOptions, error) {
	words := optionWords(claim.Text)
	var set *bool
	switch claim.Pattern {
	case executeAs:
		principal, err := readExecuteAs(claim, words)
		if err != nil || principal == "" {
			return options, err
		}
		options.Principal = principal
		return options, nil
	case schemaBinding:
		set = &options.SchemaBinding
	case recompile:
		set = &options.Recompile
	case calledOnNull:
		if !sameWords(words, "CALLED", "ON", "NULL", "INPUT") {
			return options, refusal(claim, "the option is CALLED ON NULL INPUT")
		}
		return options, nil
	case returnsNullOnNull:
		if !sameWords(words, "RETURNS", "NULL", "ON", "NULL", "INPUT") {
			return options, refusal(claim, "the option is RETURNS NULL ON NULL INPUT")
		}
		set = &options.ReturnsNullOnNullInput
	default:
		return options, fmt.Errorf("%w: SQL Server routine claim of pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
	}
	if len(words) != 1 && claim.Pattern != returnsNullOnNull {
		return options, refusal(claim, "the option takes no value")
	}
	*set = true
	return options, nil
}

// optionWords returns the tokens of an option's text, without whitespace and
// comments.
func optionWords(text string) []lexer.Token {
	var words []lexer.Token
	scan := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.SQLServer))
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			words = append(words, token)
		}
	}
	return words
}

// sameWords reports whether tokens are exactly the words given, in any letter
// case.
func sameWords(tokens []lexer.Token, words ...string) bool {
	if len(tokens) != len(words) {
		return false
	}
	for i, word := range words {
		if !tokens[i].MatchIdentifierValue(word) && !strings.EqualFold(tokens[i].Value, word) {
			return false
		}
	}
	return true
}

// readExecuteAs reads `EXECUTE AS <principal>` from its words. A principal
// written as a string is returned; a word is CALLER, OWNER or SELF, which the
// frontend reads and for which the empty string is returned, and anything else
// is refused at its position.
func readExecuteAs(claim sqlext.Claim, words []lexer.Token) (string, error) {
	// A Unicode string, N'auditor', reads as the letter and the string
	// beside it.
	if len(words) == 4 && words[2].MatchIdentifierValue("N") && words[3].Type == lexer.TokenString && words[2].End == words[3].Start {
		words = append(words[:2], words[3])
	}
	if len(words) != 3 {
		return "", refusal(claim, "EXECUTE AS takes one principal")
	}
	principal := words[2]
	switch {
	case principal.Type == lexer.TokenString:
		name, ok := lexer.StringValue(principal.Value, dialectlexer.Options(platform.SQLServer))
		if !ok {
			return "", refusal(claim, principal.Value+" is not a string the server reads")
		}
		if strings.TrimSpace(name) == "" {
			return "", refusal(claim, "an EXECUTE AS principal must name a database user")
		}
		return name, nil
	case principal.MatchIdentifierValue("CALLER"), principal.MatchIdentifierValue("OWNER"), principal.MatchIdentifierValue("SELF"):
		return "", nil
	}
	return "", refusal(claim, "EXECUTE AS names CALLER, SELF, OWNER or a user name in quotes, not "+principal.Value)
}

// refusal positions an error at the claim.
func refusal(claim sqlext.Claim, reason string) error {
	return fmt.Errorf("%w: %s of %s at position %d: %s", schemaext.ErrInvalidValue, strings.TrimSpace(claim.Text), claim.Subject, claim.Offset, reason)
}

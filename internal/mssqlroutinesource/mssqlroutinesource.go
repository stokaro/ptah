// Package mssqlroutinesource is the SQL Server owner's routine syntax in the
// SQL frontend: the principal a routine's EXECUTE AS names, read into the
// owner's facet (see [mssqlschema.ExecuteAsKind]). The frontend reads the rest
// of a routine, and the whole WITH list besides the principal, itself.
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

// executeAs names the owner's one SQL pattern, which each claim carries back.
const executeAs = "execute-as"

// SQL is the owner's contribution to the SQL frontend. It reads EXECUTE AS in
// the WITH list of a CREATE FUNCTION or CREATE PROCEDURE of a SQL Server
// document. `EXECUTE AS 'auditor'` and `EXECUTE AS N'auditor'` give the
// routine the principal as its facet; CALLER, OWNER and SELF give it none, and
// the frontend reads them as the routine's security mode or refuses them.
//
// Its claim is [mssqlschema.ExecuteAsCoverage]: a SQL Server document states
// a routine's principal in its CREATE statement, so a routine without one runs
// as no named principal.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   mssqlschema.RoutineOwner,
		Targets: []string{platform.SQLServer},
		Patterns: []sqlext.Pattern{
			{Name: executeAs, Site: sqlext.RoutineOption, Words: []string{"EXECUTE"}, Next: []string{"AS"}, Targeted: true},
		},
		Facets:   []schemaext.Kind{mssqlschema.ExecuteAsKind},
		Parser:   parser{},
		Coverage: sqlCoverage,
	}
}

// sqlCoverage is the owner's claim for a SQL Server document. The owner reads
// no header entry, so it is handed none.
func sqlCoverage(sqlext.CoverageRequest) (schemaext.Coverage, error) {
	return mssqlschema.ExecuteAsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// parser reads the spans the owner's pattern claims.
type parser struct{}

// ParseSQL reads every claim of one document.
func (parser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	seen := make(map[int]bool)
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if claim.Pattern != executeAs {
			return sqlext.Result{}, fmt.Errorf("%w: SQL Server routine claim of pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		if seen[claim.Definition] {
			return sqlext.Result{}, refusal(claim, "states EXECUTE AS twice")
		}
		seen[claim.Definition] = true
		value, err := readExecuteAs(claim)
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

// readExecuteAs reads `EXECUTE AS <principal>`. A principal written as a
// string is the facet; a word is CALLER, OWNER or SELF, which the frontend
// reads, and anything else is refused at its position.
func readExecuteAs(claim sqlext.Claim) (sqlext.Value, error) {
	var words []lexer.Token
	scan := lexer.NewLexerWithOptions(claim.Text, dialectlexer.Options(platform.SQLServer))
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			words = append(words, token)
		}
	}
	// A Unicode string, N'auditor', reads as the letter and the string
	// beside it.
	if len(words) == 4 && words[2].MatchIdentifierValue("N") && words[3].Type == lexer.TokenString && words[2].End == words[3].Start {
		words = append(words[:2], words[3])
	}
	if len(words) != 3 {
		return sqlext.Value{}, refusal(claim, "EXECUTE AS takes one principal")
	}
	principal := words[2]
	switch {
	case principal.Type == lexer.TokenString:
		name, ok := lexer.StringValue(principal.Value, dialectlexer.Options(platform.SQLServer))
		if !ok {
			return sqlext.Value{}, refusal(claim, principal.Value+" is not a string the server reads")
		}
		facets, err := mssqlschema.WithExecuteAs(schemaext.Facets{}, name)
		if err != nil {
			return sqlext.Value{}, refusal(claim, err.Error())
		}
		return sqlext.Value{Facets: facets}, nil
	case principal.MatchIdentifierValue("CALLER"), principal.MatchIdentifierValue("OWNER"), principal.MatchIdentifierValue("SELF"):
		return sqlext.Value{}, nil
	}
	return sqlext.Value{}, refusal(claim, "EXECUTE AS names CALLER, SELF, OWNER or a user name in quotes, not "+principal.Value)
}

// refusal positions an error at the claim.
func refusal(claim sqlext.Claim, reason string) error {
	return fmt.Errorf("%w: %s of %s at position %d: %s", schemaext.ErrInvalidValue, strings.TrimSpace(claim.Text), claim.Subject, claim.Offset, reason)
}

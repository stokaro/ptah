package pgtablesource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SQL reads UNLOGGED before TABLE on targets with a defined persistence
// policy, and marks a PostgreSQL document's table
// persistence as complete. An untargeted document reads the syntax without
// extending coverage to tables whose target is unknown.
func SQL() []sqlext.Extension {
	foreign := slices.DeleteFunc(pgtable.PersistenceTargets(), func(target string) bool { return target == platform.Postgres || target == platform.YDB })
	return []sqlext.Extension{
		{Owner: pgtable.Owner, Targets: []string{platform.Postgres}, Facets: []schemaext.Kind{pgtable.PersistenceKind},
			Patterns: []sqlext.Pattern{{Name: "unlogged", Site: sqlext.TablePrefix, Words: []string{"UNLOGGED"}, Targeted: true}}, Parser: persistenceSQL{},
			Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) { return Coverage() }},
		{Owner: pgtable.Owner, Targets: foreign, Facets: []schemaext.Kind{pgtable.PersistenceKind},
			Patterns: []sqlext.Pattern{{Name: "unlogged", Site: sqlext.TablePrefix, Words: []string{"UNLOGGED"}}}, Parser: persistenceSQL{}},
	}
}

type persistenceSQL struct{}

func (persistenceSQL) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, fmt.Errorf("%w: table persistence SQL requires a context", schemaext.ErrInvalidValue)
	}
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if err := readUnlogged(claim, request.Target); err != nil {
			return sqlext.Result{}, err
		}
		facets, err := pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: true})
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, sqlext.Value{Facets: facets})
	}
	if err := ctx.Err(); err != nil {
		return sqlext.Result{}, err
	}
	return sqlext.Result{Values: values}, nil
}

func readUnlogged(claim sqlext.Claim, target string) error {
	options := dialectlexer.Options(target)
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	var tokens []lexer.Token
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		tokens = append(tokens, token)
	}
	if claim.Pattern != "unlogged" || claim.Site != sqlext.TablePrefix || len(tokens) != 1 || !tokens[0].MatchIdentifierValue("UNLOGGED") {
		return fmt.Errorf("%w: expected UNLOGGED table prefix at position %d", schemaext.ErrInvalidValue, claim.Offset)
	}
	return nil
}

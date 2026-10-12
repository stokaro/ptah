package pgroutinesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SQL registers the function clauses owned by PostgreSQL. A complete SQL
// declaration without these clauses states the server defaults.
func SQL() sqlext.Extension {
	return sqlext.Extension{Owner: pgroutine.PropertiesOwner,
		Targets: []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB},
		Patterns: []sqlext.Pattern{
			{Name: "leakproof", Site: sqlext.RoutineOption, Words: []string{"LEAKPROOF"}},
			{Name: "not-leakproof", Site: sqlext.RoutineOption, Words: []string{"NOT", "LEAKPROOF"}},
			{Name: "strict", Site: sqlext.RoutineOption, Words: []string{"STRICT"}},
			{Name: "called", Site: sqlext.RoutineOption, Words: []string{"CALLED"}},
			{Name: "returns-null", Site: sqlext.RoutineOption, Words: []string{"RETURNS"}, Next: []string{"NULL"}},
			{Name: "parallel", Site: sqlext.RoutineOption, Words: []string{"PARALLEL"}},
		}, Facets: []schemaext.Kind{pgroutine.PropertiesKind}, Parser: sqlParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) { return Coverage() },
	}
}

type sqlParser struct{}

func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	result := sqlext.Result{Values: make([]sqlext.Value, len(request.Claims))}
	first := make(map[int]int)
	properties := make(map[int]pgroutine.Properties)
	seen := make(map[string]bool)
	var order []int
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if _, found := first[claim.Definition]; !found {
			first[claim.Definition] = i
			order = append(order, claim.Definition)
		}
		value, field, err := readProperty(claim, properties[claim.Definition])
		if err != nil {
			return sqlext.Result{}, err
		}
		key := fmt.Sprintf("%d/%s", claim.Definition, field)
		if seen[key] {
			return sqlext.Result{}, propertyError(claim, "the function states "+field+" twice")
		}
		seen[key] = true
		properties[claim.Definition] = value
	}
	for _, definition := range order {
		facets, err := pgroutine.WithProperties(schemaext.Facets{}, properties[definition])
		if err != nil {
			return sqlext.Result{}, propertyError(request.Claims[first[definition]], err.Error())
		}
		result.Values[first[definition]] = sqlext.Value{Facets: facets}
	}
	return result, ctx.Err()
}
func readProperty(claim sqlext.Claim, value pgroutine.Properties) (pgroutine.Properties, string, error) {
	scan := lexer.NewLexerWithOptions(claim.Text, dialectlexer.Options(platform.Postgres))
	var words []string
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			words = append(words, strings.ToUpper(token.Value))
		}
	}
	text := strings.Join(words, " ")
	switch text {
	case "LEAKPROOF":
		value.Leakproof = true
		return value, "leakproof", nil
	case "NOT LEAKPROOF":
		value.Leakproof = false
		return value, "leakproof", nil
	case "STRICT", "RETURNS NULL ON NULL INPUT":
		value.Strict = true
		return value, "strict", nil
	case "CALLED ON NULL INPUT":
		value.Strict = false
		return value, "strict", nil
	}
	if len(words) == 2 && words[0] == "PARALLEL" {
		value.Parallel = words[1]
		normalized, err := value.Normalize()
		if err == nil {
			return normalized, "parallel", nil
		}
	}
	return pgroutine.Properties{}, "", propertyError(claim, "invalid function property "+strings.TrimSpace(claim.Text))
}
func propertyError(claim sqlext.Claim, reason string) error {
	return fmt.Errorf("%w: function %s at position %d: %s", schemaext.ErrInvalidValue, claim.Subject, claim.Offset, reason)
}

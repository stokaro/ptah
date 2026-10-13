package pgtablesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgexclude"
	"ptah.run/internal/pgname"
)

func exclusionSQL() sqlext.Extension {
	return sqlext.Extension{Owner: pgtable.Owner, Targets: []string{platform.Postgres}, Kinds: []schemaext.Kind{pgtable.ExclusionKind},
		Patterns: []sqlext.Pattern{
			{Name: "exclude", Site: sqlext.TableElement, Words: []string{"EXCLUDE"}},
			{Name: "named-exclude", Site: sqlext.TableElement, Words: []string{"CONSTRAINT"}, AfterName: []string{"EXCLUDE"}},
			{Name: "add-exclude", Site: sqlext.AlterOperation, Words: []string{"ADD", "EXCLUDE"}},
			{Name: "add-named-exclude", Site: sqlext.AlterOperation, Words: []string{"ADD", "CONSTRAINT"}, AfterName: []string{"EXCLUDE"}},
		}, Parser: exclusionSQLParser{}, Comments: exclusionSQLParser{}, Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) { return exclusionCoverage() },
	}
}

type exclusionSQLParser struct{}

func (exclusionSQLParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, fmt.Errorf("%w: EXCLUDE SQL requires a context", schemaext.ErrInvalidValue)
	}
	result := sqlext.Result{Values: make([]sqlext.Value, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		value, err := exclusionSQLDeclaration(claim)
		if err != nil {
			return sqlext.Result{}, fmt.Errorf("EXCLUDE at position %d: %w", claim.Offset, err)
		}
		declaration := sqlext.Value{Constraint: value}
		if claim.Site == sqlext.AlterOperation {
			definition, ok := value.Value.(*pgtable.DesiredExclusion)
			if !ok {
				return sqlext.Result{}, fmt.Errorf("%w: invalid EXCLUDE declaration representation", schemaext.ErrInvalidValue)
			}
			declaration.Operation = &pgtable.AlterExclusion{Name: value.Name, Change: pgtable.ExclusionChange{After: definition}}
		}
		result.Values = append(result.Values, declaration)
	}
	return result, ctx.Err()
}

func exclusionSQLDeclaration(claim sqlext.Claim) (*ast.OwnedIndexConstraint, error) {
	text := claim.Text
	name := ""
	if claim.Site == sqlext.AlterOperation {
		tokens, err := partitionTokens(text)
		if err != nil {
			return nil, err
		}
		if len(tokens) < 2 || !tokens[0].MatchIdentifierValue("ADD") {
			return nil, fmt.Errorf("%w: expected ADD EXCLUDE", schemaext.ErrInvalidValue)
		}
		text = text[tokens[1].Start:]
	}
	if strings.HasSuffix(claim.Pattern, "named-exclude") {
		tokens, err := partitionTokens(text)
		if err != nil {
			return nil, err
		}
		if len(tokens) < 3 {
			return nil, fmt.Errorf("%w: expected CONSTRAINT name EXCLUDE", schemaext.ErrInvalidValue)
		}
		name = partitionName(tokens[1].Value)
		text = text[tokens[2].Start:]
	}
	definition, err := pgexclude.Parse(text)
	if err != nil {
		return nil, err
	}
	value := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{
		UsingMethod: definition.UsingMethod, Elements: definition.Elements, WhereCondition: definition.WhereCondition,
		Deferrable: definition.Deferrable, Initially: definition.Initially,
	}}
	if err := pgtable.ValidateDesiredExclusion(value); err != nil {
		return nil, err
	}
	return &ast.OwnedIndexConstraint{Name: name, Value: value,
		NameParts: pgname.ExcludeElementNames(value.Elements), NameSuffix: "excl", FoldKey: pgtable.ExclusionComparisonKey(value.Exclusion),
		Expressions: []string{value.Elements, value.WhereCondition},
	}, nil
}

// ApplySQLComments applies the host's resolved comment batch to declarations.
func (exclusionSQLParser) ApplySQLComments(ctx context.Context, request sqlext.CommentRequest) ([]schemaext.Object, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: EXCLUDE comments require a context", schemaext.ErrInvalidValue)
	}
	var objects []schemaext.Object
	for _, edit := range request.Edits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := exclusionValue(edit.Object)
		if err != nil {
			return nil, err
		}
		cloned, ok := value.Clone().(*pgtable.DesiredExclusion)
		if !ok {
			return nil, fmt.Errorf("%w: EXCLUDE clone changes representation", schemaext.ErrInvalidValue)
		}
		cloned.Comment = edit.Text
		object := edit.Object
		object.Value = cloned
		objects = append(objects, object)
	}
	return objects, ctx.Err()
}

package pgtypesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgtype"
)

const createRangePattern = "create-range"
const commentTypePattern = "comment-type"
const createDomainPattern = "create-domain"
const createCompositePattern = "create-composite"
const commentDomainPattern = "comment-domain"

// SQL reads PostgreSQL user type declarations and comments in source order.
// A comment on a type outside this owner returns to the shared SQL grammar.
func SQL(targets []string) sqlext.Extension {
	return sqlext.Extension{Owner: pgtype.Owner, Targets: targets, Kinds: pgtype.Kinds(),
		Patterns: []sqlext.Pattern{
			{Name: createRangePattern, Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS", "RANGE"}},
			{Name: createDomainPattern, Site: sqlext.Statement, Words: []string{"CREATE", "DOMAIN"}},
			{Name: createCompositePattern, Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS"}, Next: []string{"("}},
			{Name: commentDomainPattern, Site: sqlext.Statement, Words: []string{"COMMENT", "ON", "DOMAIN"}},
			{Name: commentTypePattern, Site: sqlext.Statement, Words: []string{"COMMENT", "ON", "TYPE"}},
		}, Parser: rangeSQL{}, Header: []coverage.Kind{coverage.Domain, coverage.Composite, coverage.Range},
		Coverage: func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
			return sourceCoverage(request.Limits)
		},
	}
}

type rangeSQL struct{}

func (rangeSQL) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, fmt.Errorf("%w: range SQL requires a context", schemaext.ErrInvalidValue)
	}
	known := make(map[objectidentity.Key]schemaext.Object, len(request.Declared))
	for _, object := range request.Declared {
		known[object.Ref.Key()] = object
	}
	result := sqlext.Result{Values: make([]sqlext.Value, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		tokens := newRangeTokens(request.Target, claim)
		value, err := tokens.read(claim.Pattern, known)
		if err != nil {
			return sqlext.Result{}, err
		}
		for _, object := range value.Objects {
			known[object.Ref.Key()] = object
		}
		for _, object := range value.Replaces {
			known[object.Ref.Key()] = object
		}
		result.Values = append(result.Values, value)
	}
	if err := ctx.Err(); err != nil {
		return sqlext.Result{}, err
	}
	return result, nil
}

func (t *rangeTokens) read(pattern string, known map[objectidentity.Key]schemaext.Object) (sqlext.Value, error) {
	switch pattern {
	case createRangePattern:
		return t.create()
	case createDomainPattern:
		return t.createDomain()
	case createCompositePattern:
		return t.createComposite()
	case commentTypePattern, commentDomainPattern:
		return t.typeComment(pattern, known)
	default:
		return sqlext.Value{}, fmt.Errorf("%w: range SQL pattern %q", schemaext.ErrInvalidValue, pattern)
	}
}

func (t *rangeTokens) create() (sqlext.Value, error) {
	if err := t.words("CREATE", "TYPE"); err != nil {
		return sqlext.Value{}, err
	}
	ref, err := t.reference()
	if err != nil {
		return sqlext.Value{}, err
	}
	if err := t.words("AS", "RANGE"); err != nil {
		return sqlext.Value{}, err
	}
	if err := t.punctuation("("); err != nil {
		return sqlext.Value{}, err
	}
	attributes := make(map[string]string)
	for {
		name, err := t.name("range option")
		if err != nil {
			return sqlext.Value{}, err
		}
		name = strings.ToLower(name)
		switch name {
		case "subtype", pgtype.RangeSubtypeOpClass, pgtype.RangeCollation, pgtype.RangeCanonical, pgtype.RangeSubtypeDiff:
		default:
			return sqlext.Value{}, fmt.Errorf("unsupported range option %q at position %d", name, t.offset+t.current().Start)
		}
		if _, duplicate := attributes[name]; duplicate {
			return sqlext.Value{}, fmt.Errorf("range option %q is declared more than once", name)
		}
		if err := t.punctuation("="); err != nil {
			return sqlext.Value{}, err
		}
		text, err := t.optionValue(name)
		if err != nil {
			return sqlext.Value{}, err
		}
		attributes[name] = text
		if !t.operator(",") {
			break
		}
		t.pos++
	}
	if err := t.punctuation(")"); err != nil {
		return sqlext.Value{}, err
	}
	if err := t.end(); err != nil {
		return sqlext.Value{}, err
	}
	declared := &pgtype.DesiredRange{Subtype: attributes["subtype"], SubtypeOpClass: attributes[pgtype.RangeSubtypeOpClass],
		Collation: attributes[pgtype.RangeCollation], Canonical: attributes[pgtype.RangeCanonical], SubtypeDiff: attributes[pgtype.RangeSubtypeDiff]}
	object, err := pgtype.DesiredObject(ref, declared)
	if err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: pgtype.CreateRangeOf(ref, declared), Objects: []schemaext.Object{object}}, nil
}

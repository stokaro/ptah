package pgpolicysource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgpolicy"
)

const (
	sqlPolicyCreate   = "create-policy"
	sqlPolicyComment  = "comment-policy"
	sqlPolicySwitches = "row-security"
)

// SQL reads PostgreSQL policy declarations, comments and table switches on
// the targets the provider serves. It also reads documents with no target.
// Statements retain the owner's operation payload for syntax-only readers.
// The desired-schema reader applies declarations in source order and enforces
// their table references and declaration refusals. DROP TABLE ends a table's
// policy and switch declarations, even when a later table uses the same name.
func SQL(targets []string) sqlext.Extension {
	patterns := []sqlext.Pattern{
		{Name: sqlPolicyCreate, Site: sqlext.Statement, Words: []string{"CREATE", "POLICY"}},
		{Name: sqlPolicyComment, Site: sqlext.Statement, Words: []string{"COMMENT", "ON", "POLICY"}},
	}
	for _, prefix := range [][]string{{"ALTER", "TABLE"}, {"ALTER", "TABLE", "ONLY"}} {
		for _, tail := range [][]string{{"ENABLE", "ROW", "LEVEL", "SECURITY"}, {"FORCE", "ROW", "LEVEL", "SECURITY"}, {"DISABLE", "ROW", "LEVEL", "SECURITY"}} {
			patterns = append(patterns, sqlext.Pattern{Name: sqlPolicySwitches + "-" + strings.Join(prefix, "-") + "-" + strings.Join(tail, "-"), Site: sqlext.Statement, Words: prefix, AfterName: tail})
		}
	}
	return sqlext.Extension{Owner: pgpolicy.Owner, Targets: targets, Kinds: []schemaext.Kind{pgpolicy.PolicyKind}, Facets: []schemaext.Kind{pgpolicy.TableStateKind}, Patterns: patterns, Parser: policySQL{},
		Coverage: func(request sqlext.CoverageRequest) (schemaext.Coverage, error) { return Coverage(request.Declared) },
	}
}

type policySQL struct{}

func (policySQL) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	state := policySQLState{known: make(map[objectidentity.Key]schemaext.Object), pending: make(map[objectidentity.Key]*policySwitches)}
	for _, object := range request.Declared {
		state.known[object.Ref.Key()] = object
	}
	values := make([]sqlext.Value, 0, len(request.Claims))
	drops := request.DroppedTables
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		for len(drops) > 0 && drops[0].Offset < claim.Offset {
			if err := state.drop(request.Target, drops[0]); err != nil {
				return sqlext.Result{}, err
			}
			drops = drops[1:]
		}
		tokens := newPolicyTokens(request.Target, claim)
		value, err := state.read(tokens, claim.Pattern, len(values))
		if err != nil {
			return sqlext.Result{}, err
		}
		for _, object := range value.Objects {
			state.known[object.Ref.Key()] = object
		}
		for _, object := range value.Replaces {
			if _, declared := state.known[object.Ref.Key()]; declared {
				state.known[object.Ref.Key()] = object
			}
		}
		values = append(values, value)
	}
	for _, drop := range drops {
		if err := state.drop(request.Target, drop); err != nil {
			return sqlext.Result{}, err
		}
	}
	for _, group := range state.groups {
		if err := group.finish(values); err != nil {
			return sqlext.Result{}, err
		}
	}
	return sqlext.Result{Values: values}, nil
}

type policySQLState struct {
	known   map[objectidentity.Key]schemaext.Object
	pending map[objectidentity.Key]*policySwitches
	groups  []*policySwitches
}

func (s *policySQLState) read(t *policyTokens, pattern string, index int) (sqlext.Value, error) {
	switch pattern {
	case sqlPolicyCreate:
		return t.create(s.known)
	case sqlPolicyComment:
		return t.comment(s.known)
	default:
		if strings.HasPrefix(pattern, sqlPolicySwitches+"-") {
			return s.switches(t, index)
		}
		return sqlext.Value{}, fmt.Errorf("%w: PostgreSQL policy SQL pattern %q", schemaext.ErrInvalidValue, pattern)
	}
}

func (s *policySQLState) drop(target string, drop sqlext.DroppedTable) error {
	t := newPolicyTokens(target, sqlext.Claim{Text: drop.Name, Offset: drop.Offset})
	table, err := t.table()
	if err != nil {
		return err
	}
	if err = t.end(); err != nil {
		return err
	}
	key := TableRef(table.schema, table.name).Key()
	if group, found := s.pending[key]; found {
		group.dropped = true
		delete(s.pending, key)
	}
	for keyOfPolicy, object := range s.known {
		if pgpolicy.Table(object.Ref).Key() == key {
			delete(s.known, keyOfPolicy)
		}
	}
	return nil
}

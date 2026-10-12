package pgpolicysource

import (
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgpolicy"
)

// policySwitches groups statements within one table lifetime. Every statement
// still requires its table at its own position; the last supplies the facet.
type policySwitches struct {
	table   policyTable
	state   pgpolicy.DesiredTableState
	claims  []int
	dropped bool
}

func (s *policySQLState) switches(t *policyTokens, index int) (sqlext.Value, error) {
	if err := t.words("ALTER", "TABLE"); err != nil {
		return sqlext.Value{}, err
	}
	if t.keyword("ONLY") {
		t.pos++
	}
	table, err := t.table()
	if err != nil {
		return sqlext.Value{}, err
	}
	word := t.current().Value
	t.pos++
	if err = t.words("ROW", "LEVEL", "SECURITY"); err != nil {
		return sqlext.Value{}, err
	}
	if err = t.end(); err != nil {
		return sqlext.Value{}, err
	}
	before := &pgpolicy.ObservedTableState{}
	after := &pgpolicy.DesiredTableState{}
	switch {
	case strings.EqualFold(word, "ENABLE"):
		after.Enabled = true
	case strings.EqualFold(word, "FORCE"):
		after.Forced = true
	default:
		return sqlext.Value{}, fmt.Errorf("unsupported ALTER operation: %s ROW LEVEL SECURITY at position %d", word, t.offset)
	}
	access := pgpolicy.TableStateAccess(before, after)
	if after.Forced {
		access = schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "whether the table enables row security is not established by FORCE"}
	}
	value := sqlext.Value{Statement: &pgpolicy.TableStateOperation{Schema: table.schema, Table: table.name,
		Change: pgpolicy.TableStateChange{Before: before, After: after, Access: access}}, TableReferences: []string{table.written}}
	key := TableRef(table.schema, table.name).Key()
	group, found := s.pending[key]
	if !found {
		group = &policySwitches{table: table}
		s.pending[key] = group
		s.groups = append(s.groups, group)
	}
	if group.state.Enabled && after.Enabled {
		return sqlext.Value{}, fmt.Errorf("table %s declares ENABLE ROW LEVEL SECURITY twice; keep one declaration", table.written)
	}
	group.state.Enabled = group.state.Enabled || after.Enabled
	group.state.Forced = group.state.Forced || after.Forced
	group.claims = append(group.claims, index)
	return value, nil
}

func (s *policySwitches) finish(values []sqlext.Value) error {
	last := s.claims[len(s.claims)-1]
	value := &values[last]
	if !s.state.Enabled && !s.dropped {
		value.UnmodeledReason = fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY has no effect until the table enables row-level security; declare ALTER TABLE %s ENABLE ROW LEVEL SECURITY too", s.table.written, s.table.written)
	}
	facets, err := schemaext.NewFacets(&s.state)
	if err != nil {
		return err
	}
	value.TableFacets = &sqlext.TableFacets{Name: s.table.written, Facets: facets}
	for _, index := range s.claims[:len(s.claims)-1] {
		values[index].DescribedBy = last + 1
	}
	return nil
}

package atlasschema

import (
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
)

// rehearsedState is current as the comparison read it: each constraint an
// exclude selector took out of current that the comparison read all the same
// is back among its constraints. compared is the comparison of the desired
// schema with current.
//
// A comparison reads a foreign key out of scope when the desired schema
// declares it: `--exclude t1` takes `t2 REFERENCES t1` out of the read and
// leaves it in the declaration. [difftypes.SchemaDiff.CurrentOutOfScope] holds
// the parts the comparison did not read, so such a key is the one current
// holds out of scope and compared does not. A plan that changes the key drops
// it and adds it again, and the dev database rehearsal recreates this state
// before it runs the plan: recreated without the key, the dev database
// refuses the DROP, with Error 1091 on MySQL and SQLSTATE 42704 on PostgreSQL
// (stokaro/ptah#6847). The table the key references stays out of the state;
// the rehearsal adds it from the target, as it adds every object outside the
// compared scope that the state or a statement needs.
//
// current is returned as it is when the comparison read every part it holds
// out of scope as out of scope, and is never modified.
func rehearsedState(current *catalog.Database, compared *difftypes.SchemaDiff) *catalog.Database {
	if current == nil || compared == nil || len(current.OutOfScope.Constraints) == len(compared.CurrentOutOfScope.Constraints) {
		return current
	}
	unread := make(map[string]int, len(compared.CurrentOutOfScope.Constraints))
	for _, constraint := range compared.CurrentOutOfScope.Constraints {
		unread[constraintPart(constraint)]++
	}
	state := *current
	state.Constraints = slices.Clone(current.Constraints)
	state.OutOfScope.Constraints = nil
	for _, constraint := range current.OutOfScope.Constraints {
		if part := constraintPart(constraint); unread[part] > 0 {
			unread[part]--
			state.OutOfScope.Constraints = append(state.OutOfScope.Constraints, constraint)
			continue
		}
		state.Constraints = append(state.Constraints, constraint)
	}
	return &state
}

// constraintPart names a constraint by everything that tells two constraints
// of one read apart, so an unnamed key is told from another unnamed key of
// its table by the columns it holds and the table it references.
func constraintPart(constraint catalog.Constraint) string {
	return fmt.Sprintf("%q", []string{
		constraint.Schema, constraint.TableName, constraint.Name, constraint.Type,
		fmt.Sprint(constraint.ColumnNamesOrDefault()),
		constraint.ForeignSchema, derefOr(constraint.ForeignTable), fmt.Sprint(constraint.ForeignColumnsOrDefault()),
	})
}

// derefOr is the value s points to, or the empty string for nil.
func derefOr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

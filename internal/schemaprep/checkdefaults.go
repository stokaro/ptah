package schemaprep

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
)

// AssignDefaultCheckNames returns database with a name on every table CHECK it
// leaves unnamed, on a dialect whose server refuses a CHECK without one
// ([descriptor.CheckNamingRequired]). On any other dialect it returns database
// as it is, since that server names the CHECK itself and the comparison looks
// for the name it gives.
//
// The name is the one an entry of the table's checks list takes:
// `<table>_check`, then `<table>_check1` and on, past the names the table's
// constraints declare and the names its columns' CHECKs take. A render, a plan
// and a comparison of the same schema all read the CHECK under that name, so the
// constraint the first apply creates is the one the next comparison finds.
// Left unnamed, the comparison keys the declared CHECK by its definition and
// the one the server holds by the name the renderer gave it, and the two never
// pair.
//
// The input is not modified; a database that needs no name is returned as it
// is, and one that does is a copy with its own constraint list.
func AssignDefaultCheckNames(database *schemamodel.Database, dialect string) *schemamodel.Database {
	// With no pairing there is nothing to fail, and nothing reads the context.
	assigned, _ := assignCheckNames(context.Background(), database, dialect, nil)
	return assigned
}

// HeldCheck is a CHECK constraint a compared database holds on a table.
type HeldCheck struct {
	// Name is the name the database holds the CHECK under.
	Name string
	// Expression is its condition, as the database reports it.
	Expression string
}

// CheckPairing tells [AssignCheckNamesAgainst] which CHECKs a compared
// database holds and which of them a declared CHECK is.
type CheckPairing interface {
	// Held returns the CHECKs the database holds on table, in a fixed order.
	Held(table schemamodel.Table) []HeldCheck
	// Match answers, for each of expressions declared on table, which of
	// candidates is the same CHECK: alike[i][j] reports whether
	// expressions[i] and candidates[j] are one constraint, and alike holds
	// one row of len(candidates) entries for each expression. candidates are
	// CHECKs Held returned that no declaration names. One call answers every
	// unnamed CHECK of the table, so a pairing that asks a server asks it
	// once for the table.
	Match(ctx context.Context, table schemamodel.Table, expressions []string, candidates []HeldCheck) (alike [][]bool, err error)
}

// AssignCheckNamesAgainst is [AssignDefaultCheckNames] for a comparison with a
// database that pairing describes. Before the numbering, each unnamed table
// CHECK takes the name of the first held CHECK of its table that pairing
// matches, among those no declared CHECK names and no other unnamed CHECK has
// taken. The numbering then names the rest past every name taken so far.
//
// Without the pairing, the names follow the order of the declarations, so a
// swap of two unnamed CHECKs, or a named CHECK beside them that takes the first
// name, gives every CHECK after it another name, and the comparison drops and
// adds each one. An error from Match is returned with a nil database.
func AssignCheckNamesAgainst(
	ctx context.Context, database *schemamodel.Database, dialect string, pairing CheckPairing,
) (*schemamodel.Database, error) {
	return assignCheckNames(ctx, database, dialect, pairing)
}

// assignCheckNames names the unnamed table CHECKs of database as
// [AssignCheckNamesAgainst] says, and as [AssignDefaultCheckNames] says when
// pairing is nil.
func assignCheckNames(ctx context.Context, database *schemamodel.Database, dialect string, pairing CheckPairing) (*schemamodel.Database, error) {
	if database == nil || describe(dialect).checkNaming != descriptor.CheckNamingRequired {
		return database, nil
	}
	if !slices.ContainsFunc(database.Constraints, unnamedCheck) {
		return database, nil
	}
	assigned := *database
	assigned.Constraints = slices.Clone(database.Constraints)
	for _, table := range assigned.Tables {
		taken := declaredConstraintNames(table, assigned.Constraints)
		for _, name := range ColumnCheckNames(table, assigned.Fields, assigned.Constraints, dialect) {
			taken[name] = struct{}{}
		}
		if pairing != nil {
			if err := pairHeldChecks(ctx, &assigned, table, taken, pairing); err != nil {
				return nil, err
			}
		}
		numberTableChecks(&assigned, table, taken)
	}
	return &assigned, nil
}

// pairHeldChecks gives each unnamed CHECK of table the name of the held CHECK
// pairing matches, as [AssignCheckNamesAgainst] says, and adds the names it
// gives to taken.
func pairHeldChecks(
	ctx context.Context, database *schemamodel.Database, table schemamodel.Table, taken map[string]struct{}, pairing CheckPairing,
) error {
	var candidates []HeldCheck
	for _, held := range pairing.Held(table) {
		if _, declared := taken[held.Name]; !declared && strings.TrimSpace(held.Name) != "" {
			candidates = append(candidates, held)
		}
	}
	var unnamed []int
	var expressions []string
	for i, constraint := range database.Constraints {
		if !unnamedCheck(constraint) || !ConstraintBelongsToTable(constraint, table) || strings.TrimSpace(constraint.CheckExpression) == "" {
			continue
		}
		unnamed = append(unnamed, i)
		expressions = append(expressions, constraint.CheckExpression)
	}
	if len(candidates) == 0 || len(unnamed) == 0 {
		return nil
	}
	alike, err := pairing.Match(ctx, table, expressions, candidates)
	if err != nil {
		return err
	}
	if len(alike) != len(expressions) {
		return fmt.Errorf("name the CHECKs of %s: the pairing answered %d of %d declarations", table.QualifiedName(), len(alike), len(expressions))
	}
	paired := make([]bool, len(candidates))
	for k, i := range unnamed {
		for j, held := range candidates {
			if paired[j] || j >= len(alike[k]) || !alike[k][j] {
				continue
			}
			database.Constraints[i].Name = held.Name
			taken[held.Name] = struct{}{}
			paired[j] = true
			break
		}
	}
	return nil
}

// numberTableChecks names each CHECK of table still unnamed `<table>_check`,
// then `<table>_check1` and on, past the names in taken, and adds them to it.
func numberTableChecks(database *schemamodel.Database, table schemamodel.Table, taken map[string]struct{}) {
	ordinal := 0
	for i, constraint := range database.Constraints {
		if !unnamedCheck(constraint) || !ConstraintBelongsToTable(constraint, table) {
			continue
		}
		name := tableCheckConstraintName(table.Name, ordinal)
		for {
			if _, used := taken[name]; !used {
				break
			}
			ordinal++
			name = tableCheckConstraintName(table.Name, ordinal)
		}
		taken[name] = struct{}{}
		ordinal++
		database.Constraints[i].Name = name
	}
}

// AssignDefaultConstraintNames is [AssignDefaultForeignKeyNames] followed by
// [AssignDefaultCheckNames]: the names every path that renders, plans or
// compares a schema for dialect gives the constraints the schema leaves
// unnamed. The input is not modified.
func AssignDefaultConstraintNames(database *schemamodel.Database, dialect string) *schemamodel.Database {
	return AssignDefaultCheckNames(AssignDefaultForeignKeyNames(database, dialect), dialect)
}

// AssignConstraintNamesAgainst is [AssignDefaultConstraintNames] for a
// comparison with the database pairing describes: the foreign keys are named
// as there, and the CHECKs as [AssignCheckNamesAgainst] names them. The input
// is not modified.
func AssignConstraintNamesAgainst(
	ctx context.Context, database *schemamodel.Database, dialect string, pairing CheckPairing,
) (*schemamodel.Database, error) {
	return AssignCheckNamesAgainst(ctx, AssignDefaultForeignKeyNames(database, dialect), dialect, pairing)
}

// unnamedCheck reports a CHECK constraint declared without a name.
func unnamedCheck(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(strings.TrimSpace(constraint.Type), "CHECK") && strings.TrimSpace(constraint.Name) == ""
}

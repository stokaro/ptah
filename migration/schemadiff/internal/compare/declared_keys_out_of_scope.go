package compare

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// WithDeclaredKeysOutOfScope returns database with each foreign key out of
// scope that desired declares back in scope, so the comparison pairs the two.
//
// An exclude selector takes a key out of the read when it names the key, or
// when it leaves out the table or the columns the key references. The desired
// side loses the key in the first case too, since it holds the key's table.
// It cannot lose it in the second, because a desired schema holds no table it
// references from outside: `--exclude t1` takes `t2 REFERENCES t1` out of the
// read and leaves it in the declaration. Out of scope, the key the server
// holds would read as missing, and every plan would add it again. Asked on
// MySQL 8.4, the second apply planned `DROP INDEX t2_fk` and `ADD CONSTRAINT
// t2_fk` and failed with Error 1553 (stokaro/ptah#6773).
//
// Only a key the comparison would pair with a declared one comes back, by the
// comparison's own pairing. A key the declaration leaves out stays out of
// scope, so the plan never drops a key into an excluded table it was not asked
// about. A key out of scope is on a table the read keeps
// ([catalog.Database.OutOfScope]), or on one [WithoutBlockedTables] took out,
// which desired does not declare; either way a declared key pairs only with a
// key of a table both sides hold. database is returned unchanged when no key
// comes back, and is never modified.
func WithDeclaredKeysOutOfScope(desired *schemamodel.Database, database *catalog.Database, dialect string, semantics identifier.Semantics) *catalog.Database {
	if desired == nil || database == nil {
		return database
	}
	type keyRef struct{ table, name string }
	refOf := func(constraint catalog.Constraint) keyRef {
		return keyRef{table: semantics.QualifiedTableIdentityKey(constraint.QualifiedTableName()), name: constraint.Name}
	}
	candidates := make(map[keyRef]bool)
	var outOfScope []catalog.Constraint
	for _, constraint := range database.OutOfScope.Constraints {
		if strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			candidates[refOf(constraint)] = true
			outOfScope = append(outOfScope, constraint)
		}
	}
	if len(outOfScope) == 0 {
		return database
	}
	probe := *database
	probe.Constraints = slices.Concat(database.Constraints, outOfScope)
	declared, read := pairedConstraints(desired, &probe, dialect, semantics)
	restored := make(map[keyRef]bool)
	for key, constraint := range read {
		if _, paired := declared[key]; paired && candidates[refOf(constraint)] {
			restored[refOf(constraint)] = true
		}
	}
	if len(restored) == 0 {
		return database
	}
	result := *database
	result.Constraints = slices.Clone(database.Constraints)
	result.OutOfScope.Constraints = nil
	for _, constraint := range database.OutOfScope.Constraints {
		if strings.EqualFold(constraint.Type, "FOREIGN KEY") && restored[refOf(constraint)] {
			result.Constraints = append(result.Constraints, constraint)
			continue
		}
		result.OutOfScope.Constraints = append(result.OutOfScope.Constraints, constraint)
	}
	return &result
}

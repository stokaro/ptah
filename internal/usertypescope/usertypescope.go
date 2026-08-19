// Package usertypescope refuses a declared PostgreSQL user type a target
// cannot host, before any SQL is rendered.
package usertypescope

import (
	"cmp"
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// ValidateDeclared refuses a schema declaring a domain, composite type or
// range type the target cannot create.
//
// A named skip is not enough here, and that is measured rather than assumed.
// Gating the type alone on CockroachDB v26.2.5 left the declaration's own
// table behind -- `CREATE TABLE people ("addr" email)` -- which the server
// then refused with `type "email" does not exist`. The skip moved the failure
// from one server error to another instead of moving it before SQL, so this
// refuses the way mysqllike.ValidateDeclaredRoles does: name the object, name
// the reason, refuse (stokaro/ptah#1717).
//
// The three kinds are asked separately because they do not travel together:
// CockroachDB takes a composite and refuses a domain and a range.
func ValidateDeclared(dialect string, caps capability.Capabilities, database *goschema.Database) error {
	if database == nil {
		return nil
	}
	for _, kind := range []struct {
		key       capability.Capability
		statement string
		names     []string
	}{
		{capability.DomainTypes, "CREATE DOMAIN", domainNames(database)},
		{capability.CompositeTypes, "CREATE TYPE ... AS (...)", compositeNames(database)},
		{capability.RangeTypes, "CREATE TYPE ... AS RANGE", rangeNames(database)},
	} {
		if caps.Has(kind.key) || len(kind.names) == 0 {
			continue
		}
		// The lexicographically first name, not the first parsed: two gates
		// answering the same schema must name the same object, and parse order
		// moves whenever a declaration is reordered.
		first := slices.MinFunc(kind.names, cmp.Compare)
		return fmt.Errorf(
			"%w: %s: %s %s: this target does not create it, and a column declared with the type "+
				"would be left naming something the server has no definition of; "+
				"scope the declaration to the dialects that host it",
			ptaherr.ErrUnsupportedFeature, dialect, kind.statement, first)
	}
	return nil
}

func domainNames(database *goschema.Database) []string {
	names := make([]string, 0, len(database.Domains))
	for _, domain := range database.Domains {
		names = append(names, domain.Name)
	}
	return names
}

func compositeNames(database *goschema.Database) []string {
	names := make([]string, 0, len(database.CompositeTypes))
	for _, composite := range database.CompositeTypes {
		names = append(names, composite.Name)
	}
	return names
}

func rangeNames(database *goschema.Database) []string {
	names := make([]string, 0, len(database.Ranges))
	for _, rangeType := range database.Ranges {
		names = append(names, rangeType.Name)
	}
	return names
}

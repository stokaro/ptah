package mysql

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
)

// columnTarget is one column of one object, as the privileges of one role name
// it. The object and the column are keyed the way the target compares them, so
// `Name` and `name` are one column on MySQL and MariaDB.
type columnTarget struct {
	role, objectType, object, column string
}

// columnSpellings is the spelling the catalog gives each column a role holds a
// privilege on, and the columns it gives two spellings.
//
// MySQL and MariaDB compare column names without case, and so does the
// server's in-memory copy of the column privileges, but mysql.columns_priv
// keeps one row per spelling. Measured on MySQL 8.4.13 and 26.10.0 and MariaDB
// 11.8.9 and 12.3.3: after `GRANT SELECT (Name) ON t TO r` and
// `GRANT UPDATE (name) ON t TO r`, COLUMN_PRIVILEGES reports both spellings,
// and `REVOKE SELECT (Name) ON t FROM r` leaves a user holding r able to read
// the column and unable to update it, while the table holds `name: Update`.
// So on MariaDB a statement names a column the way the catalog already does,
// and on both a plan that touches a role's privileges on an object where the
// catalog holds one column under two spellings is refused.
//
// On MySQL a statement names a column the way the declaration does: a column
// privilege held through a role serves only the spelling the GRANT used (see
// [platform.ColumnPrivilegeIsBySpelling]), so the comparison holds a privilege
// on `Name` and a declared one on `name` apart, and the plan revokes the first
// before it grants the second, whoever holds it. The revokes run first, and
// the declaration spells each column one way, so a plan leaves the catalog
// holding one spelling of a declared column privilege.
type columnSpellings struct {
	semantics identifier.Semantics
	// bySpelling is set on a target where a column privilege serves only the
	// spelling it was granted under.
	bySpelling bool
	spelled    map[columnTarget]string
	// twice holds the columns the catalog spells two ways, in the order the
	// read reports them, so a refusal names the same column on every run.
	twice []spelledTwice
}

// spelledTwice is a column the catalog gives two spellings for one role.
type spelledTwice struct {
	target    columnTarget
	spellings [2]string
}

// newColumnSpellings reads the spellings from the grants the database holds.
func newColumnSpellings(diff *difftypes.SchemaDiff, dialect string) columnSpellings {
	spellings := columnSpellings{
		semantics:  diff.EffectiveIdentifierSemantics(dialect),
		bySpelling: platform.ColumnPrivilegeIsBySpelling(dialect),
		spelled:    make(map[columnTarget]string),
	}
	for _, grant := range diff.CurrentGrants {
		column := strings.TrimSpace(grant.Column)
		if column == "" {
			continue
		}
		target := spellings.target(grant)
		previous, seen := spellings.spelled[target]
		switch {
		case !seen:
			spellings.spelled[target] = column
		case previous != column:
			spellings.twice = append(spellings.twice, spelledTwice{target: target, spellings: [2]string{previous, column}})
		}
	}
	return spellings
}

// target is the column grant names.
func (s columnSpellings) target(grant difftypes.GrantRef) columnTarget {
	return columnTarget{
		role:       strings.TrimSpace(grant.Role),
		objectType: strings.ToUpper(strings.TrimSpace(grant.ObjectType)),
		object:     s.semantics.QualifiedTableIdentityKey(grant.ObjectName),
		column:     s.semantics.ColumnIdentityKey(strings.TrimSpace(grant.Column)),
	}
}

// columns is the column list of the statement grant plans: none for a grant on
// the whole object, the grant's own spelling on a target that serves a column
// privilege by its spelling, and otherwise the catalog's spelling of the
// column where the role holds a privilege on it, the grant's own where it
// holds none.
func (s columnSpellings) columns(grant difftypes.GrantRef) []string {
	if grant.Column == "" {
		return nil
	}
	if spelled, held := s.spelled[s.target(grant)]; held && !s.bySpelling {
		return []string{spelled}
	}
	return []string{grant.Column}
}

// refuseTwoSpellings refuses a diff that changes a privilege of a role on an
// object where the catalog holds one of its columns under two spellings for
// that role.
func (s columnSpellings) refuseTwoSpellings(diff *difftypes.SchemaDiff, dialect string) error {
	if len(s.twice) == 0 {
		return nil
	}
	for _, refs := range [][]difftypes.GrantRef{
		diff.GrantsAdded, diff.GrantOptionsAdded, diff.GrantsRemoved, diff.GrantOptionsRevoked,
	} {
		for _, ref := range refs {
			changed := s.target(ref)
			for _, twice := range s.twice {
				target := twice.target
				if target.role != changed.role || target.objectType != changed.objectType || target.object != changed.object {
					continue
				}
				return fmt.Errorf(
					"%w: role %q holds privileges on one column of %s as %q and as %q; %s keeps the privileges "+
						"of a column in memory under one name, ignoring case, and in mysql.columns_priv under each "+
						"spelling, so a GRANT or REVOKE on that column changes one and not the other; revoke the "+
						"privileges under both spellings by hand and plan again",
					ptaherr.ErrUnsupportedFeature, ref.Role, ref.ObjectName, twice.spellings[0], twice.spellings[1],
					strings.ToUpper(platform.NormalizeDialect(dialect)),
				)
			}
		}
	}
	return nil
}

// refuseASecondSpelling refuses a diff that grants a role a privilege on a
// column under one spelling while the role keeps other privileges on that
// column under another.
//
// On MySQL a plan grants a column privilege as the declaration spells it, and
// it revokes a held privilege the declaration names under another spelling.
// A role the schema does not create keeps the privileges the declaration says
// nothing about, so a grant of SELECT (name) beside a kept UPDATE (Name)
// leaves the catalog holding the column under two spellings, and
// [columnSpellings.refuseTwoSpellings] refuses the next plan that touches the
// role's privileges on the object. Measured on MySQL 8.4.13 and 26.10.0:
// planned, the next apply exits 2 with that refusal.
//
// Granting under the catalog's spelling instead would give a privilege that a
// user writing the column as declared cannot use (ERROR 1143), so the plan is
// refused before it runs. The declaration has two ways out: name the kept
// privileges under the declared spelling, or revoke them. On MariaDB a grant
// takes the catalog's spelling of a column the role holds a privilege on, so
// this never fires there.
func (s columnSpellings) refuseASecondSpelling(diff *difftypes.SchemaDiff, dialect string) error {
	removed := make(map[columnGrant][]string)
	for _, grant := range diff.GrantsRemoved {
		if grant.Column != "" {
			key := s.columnGrant(grant)
			removed[key] = append(removed[key], strings.TrimSpace(grant.Column))
		}
	}
	for _, added := range diff.GrantsAdded {
		columns := s.columns(added)
		if len(columns) == 0 {
			continue
		}
		target := s.target(added)
		spelling, kept := "", []string(nil)
		for _, held := range diff.CurrentGrants {
			column := strings.TrimSpace(held.Column)
			if column == "" || column == columns[0] || s.target(held) != target ||
				slices.Contains(removed[s.columnGrant(held)], column) {
				continue
			}
			if spelling == "" {
				spelling = column
			}
			if column == spelling {
				kept = append(kept, strings.ToUpper(strings.TrimSpace(held.Privilege)))
			}
		}
		if len(kept) == 0 {
			continue
		}
		slices.Sort(kept)
		return fmt.Errorf(
			"%w: role %q keeps %s on one column of %s as %q, and the plan grants %s on it as %q, the declared "+
				"spelling; %s would then hold the column under two spellings for the role, and a later plan on %s "+
				"is refused for that -- declare the role's privileges on the column as %q too, or revoke them",
			ptaherr.ErrUnsupportedFeature, added.Role, strings.Join(slices.Compact(kept), ", "), added.ObjectName,
			spelling, strings.ToUpper(strings.TrimSpace(added.Privilege)), columns[0],
			strings.ToUpper(platform.NormalizeDialect(dialect)), added.ObjectName, columns[0],
		)
	}
	return nil
}

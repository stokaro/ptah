package mysql

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/tableref"
)

// denies plans SQL Server's DENY rows: the ones the diff adds
// ([difftypes.SchemaDiff.DeniesAdded]) and the ones it takes away
// ([difftypes.SchemaDiff.DeniesRemoved]). It compares the read's names with
// the schema's, which may leave the target's default schema out.
//
// The server keeps one state for a permission of one grantee on one securable,
// so a DENY takes the place of a GRANT and a REVOKE takes a DENY. A statement
// on a whole object also reaches the rows of the permission on its columns.
// Measured on SQL Server 2025:
//
//   - `DENY SELECT ON t` after `GRANT SELECT (c) ON t` leaves one DENY row for
//     the table, and so does `DENY SELECT ON t` after `DENY SELECT (c) ON t`;
//   - `REVOKE SELECT ON t` after `DENY SELECT ON t` and `GRANT SELECT (c) ON t`
//     leaves no row;
//   - `GRANT SELECT (c) ON t` after `DENY SELECT ON t` keeps both rows, as does
//     `DENY SELECT (c) ON t` after `GRANT SELECT ON t`;
//   - `GRANT SELECT ON t`, with the grant option or without, and `REVOKE
//     SELECT ON t` after `DENY SELECT (c) ON t` leave no DENY row for c, and
//     `REVOKE GRANT OPTION FOR SELECT ON t` keeps it;
//   - `REVOKE SELECT (c) ON t` of a DENY on c under `GRANT SELECT ON t` leaves
//     a REVOKE row for c, and `GRANT SELECT (c) ON t` with the table grant's
//     option leaves the table's row alone;
//   - a DENY of a permission the grantee holds WITH GRANT OPTION on the
//     securable, or on one of the columns a DENY on the whole object reaches,
//     answers Msg 4611 without CASCADE, and CASCADE takes what the grantee
//     granted on the securable as a whole and on the columns the DENY
//     reaches, and leaves what it granted on another column.
type denies struct {
	semantics     identifier.Semantics
	defaultSchema string
}

// newDenies reads how the target compares names, and its default schema.
func (p *Planner) newDenies(diff *difftypes.SchemaDiff) denies {
	d := denies{semantics: diff.EffectiveIdentifierSemantics(p.targetDialect())}
	if described, ok := platform.Lookup(p.targetDialect()); ok {
		d.defaultSchema = described.DefaultSchema
	}
	return d
}

// deniedKey is one permission of one grantee on one securable, or on one
// column of an object, keyed the way the target compares names.
type deniedKey struct {
	role, privilege, objectType, object, column string
}

// key is the securable and permission ref names, with the default schema left
// out of an object's name, so `t1` and `dbo.t1` key alike.
func (d denies) key(ref difftypes.GrantRef) deniedKey {
	objectType := strings.ToUpper(strings.TrimSpace(ref.ObjectType))
	object := d.semantics.QualifiedTableIdentityKey(ref.ObjectName)
	if parsed, ok := tableref.Parse(ref.ObjectName); ok && objectType != "SCHEMA" &&
		parsed.Qualified && d.semantics.QualifiedTableIdentityKey(parsed.Schema) == d.semantics.QualifiedTableIdentityKey(d.defaultSchema) {
		object = d.semantics.QualifiedTableIdentityKey(parsed.Name)
	}
	return deniedKey{
		role:       ref.Role,
		privilege:  strings.ToUpper(strings.TrimSpace(ref.Privilege)),
		objectType: objectType,
		object:     object,
		column:     d.semantics.ColumnIdentityKey(strings.TrimSpace(ref.Column)),
	}
}

// onWholeObject is key without its column.
func (k deniedKey) onWholeObject() deniedKey {
	k.column = ""
	return k
}

// reaches reports whether a statement on k reaches the row other keys on the
// same object: every row of the permission for a statement on the whole
// object, and the object's row and its own column's for one on a column.
func (k deniedKey) reaches(other deniedKey) bool {
	return k.column == "" || other.column == "" || other.column == k.column
}

// revokeDenies emits a REVOKE for each DENY the diff takes away. It runs with
// the other revokes, at the start of the plan; a REVOKE of a DENY on the whole
// object takes the permission's column rows too, and the column grants the
// diff keeps come back after them ([columnGrantsRevokedWithTheirTable]). A
// DENY on an object dropped holds goes with the object.
//
// A DENY on a column whose grantee keeps the permission on the whole object
// is taken with a GRANT on the column instead, with the grant option the
// object's grant keeps: a REVOKE there leaves a row that withholds the
// column, and the GRANT leaves the object's row alone.
func (p *Planner) revokeDenies(result []ast.Node, diff *difftypes.SchemaDiff, dropped droppedObjects) []ast.Node {
	d := p.newDenies(diff)
	for _, denied := range p.revokedDeniesByGrantor(diff) {
		if dropped.holds(denied) {
			continue
		}
		if held, kept := d.keptOnWholeObject(diff, denied, dropped); kept {
			result = append(result, ast.NewGrantPrivilege(denied.Role, denied.ObjectType, denied.ObjectName, []string{denied.Privilege}).
				SetColumns(grantColumns(denied)).
				SetWithOption(held.WithOption && !removes(diff.GrantOptionsRevoked, held)).
				SetGrantedBy(p.revokeGrantor(denied)))
			continue
		}
		result = append(result, ast.NewRevokePrivilege(denied.Role, denied.ObjectType, denied.ObjectName, []string{denied.Privilege}).
			SetColumns(grantColumns(denied)).
			SetGrantedBy(p.revokeGrantor(denied)))
	}
	return result
}

// keptOnWholeObject is the grant of denied's permission on its whole object
// the grantee holds from the same grantor and the diff keeps, for a DENY on a
// column. A synthetic inverse with no captured grantor keeps the default
// grantor behavior of the statement it reverses.
func (d denies) keptOnWholeObject(diff *difftypes.SchemaDiff, denied difftypes.GrantRef, dropped droppedObjects) (difftypes.GrantRef, bool) {
	if denied.Column == "" {
		return difftypes.GrantRef{}, false
	}
	want := d.key(denied).onWholeObject()
	for _, held := range diff.CurrentGrants {
		if d.key(held) == want && (denied.GrantedBy == "" || held.GrantedBy == denied.GrantedBy) &&
			!dropped.holds(held) && !removes(diff.GrantsRemoved, held) {
			return held, true
		}
	}
	return difftypes.GrantRef{}, false
}

// planDenies is the DENY of each permission the diff adds, split by where the
// plan writes it. onObjects runs before the grants the plan adds, and
// onColumns after them: a DENY on a whole object takes the grants on its
// columns, and a GRANT on a whole object takes the denies on its columns, so
// each statement goes where the server keeps both rows. A schema read in
// statement order folds them the same way.
//
// A DENY of a permission the grantee holds WITH GRANT OPTION when it runs
// carries CASCADE, which the server requires. CASCADE also takes the
// permission from whoever the grantee granted it to, so a plan where someone
// holds it from the grantee and the diff does not revoke it is refused.
func (p *Planner) planDenies(diff *difftypes.SchemaDiff) (onObjects, onColumns []ast.Node, err error) {
	if len(diff.DeniesAdded) == 0 {
		return nil, nil, nil
	}
	d := p.newDenies(diff)
	dropped := p.droppedObjects(diff)
	granted := p.grantsBeforeColumnDenies(diff)
	for _, denied := range diff.DeniesAdded {
		if denied.Column == "" {
			node, err := d.deny(diff, denied, dropped, nil)
			if err != nil {
				return nil, nil, err
			}
			onObjects = append(onObjects, node)
			continue
		}
		node, err := d.deny(diff, denied, dropped, granted)
		if err != nil {
			return nil, nil, err
		}
		onColumns = append(onColumns, node)
	}
	return onObjects, onColumns, nil
}

// grantsBeforeColumnDenies is the grants the plan runs before a DENY on a
// column: the ones the diff adds, the grant options it adds, and the column
// grants a DENY on their object takes ([Planner.columnGrantsDeniesTake]). A
// grant option added is marked WithOption, as the GRANT that adds it is.
func (p *Planner) grantsBeforeColumnDenies(diff *difftypes.SchemaDiff) []difftypes.GrantRef {
	granted := slices.Concat(diff.GrantsAdded, p.columnGrantsDeniesTake(diff))
	for _, option := range diff.GrantOptionsAdded {
		option.WithOption = true
		granted = append(granted, option)
	}
	return granted
}

// deny is the DENY of denied, with CASCADE where [denies.cascades] asks for
// it once granted has run.
func (d denies) deny(diff *difftypes.SchemaDiff, denied difftypes.GrantRef, dropped droppedObjects, granted []difftypes.GrantRef) (*ast.DenyPrivilegeNode, error) {
	cascade, err := d.cascades(diff, denied, dropped, granted)
	if err != nil {
		return nil, err
	}
	return ast.NewDenyPrivilege(denied.Role, denied.ObjectType, denied.ObjectName, []string{denied.Privilege}).
		SetColumns(grantColumns(denied)).
		SetCascade(cascade), nil
}

// cascades reports whether the DENY of denied needs CASCADE: when it runs,
// after the revokes and after granted, the grantee holds the permission WITH
// GRANT OPTION on the securable, on the whole object or on a column the DENY
// reaches. A grant held and kept counts, and so does one of granted, which the
// plan runs before the DENY: measured on SQL Server 2025, `DENY SELECT (c) ON
// t` right after `GRANT SELECT ON t ... WITH GRANT OPTION` answers Msg 4611.
//
// It refuses the DENY where someone holds the permission from the grantee
// where the DENY reaches and the diff keeps it: a DENY on a column takes what
// the grantee granted on the whole object and on that column, and leaves what
// it granted on another column.
func (d denies) cascades(diff *difftypes.SchemaDiff, denied difftypes.GrantRef, dropped droppedObjects, granted []difftypes.GrantRef) (bool, error) {
	want := d.key(denied)
	reached := func(grant difftypes.GrantRef) bool {
		key := d.key(grant)
		return key == want || key.onWholeObject() == want || (want.column != "" && key == want.onWholeObject())
	}
	kept := func(held difftypes.GrantRef) bool {
		return !dropped.holds(held) && !removes(diff.GrantsRemoved, held)
	}
	grantable := slices.ContainsFunc(diff.CurrentGrants, func(held difftypes.GrantRef) bool {
		return held.WithOption && reached(held) && kept(held) && !removes(diff.GrantOptionsRevoked, held)
	}) || slices.ContainsFunc(granted, func(grant difftypes.GrantRef) bool {
		return grant.WithOption && reached(grant) && !dropped.holds(grant)
	})
	if !grantable {
		return false, nil
	}
	dependents := d.dependents(diff, denied, kept)
	if len(dependents) == 0 {
		return true, nil
	}
	return false, fmt.Errorf(
		"%w: sqlserver: cannot deny %s on %s to %q: %q holds it WITH GRANT OPTION, so SQL Server denies it with "+
			"CASCADE, which also takes the %s that %q granted to %s, and the schema does not revoke those grants; "+
			"revoke them first, or declare their REVOKE in the schema",
		ptaherr.ErrUnsupportedFeature, denied.Privilege, denied.ObjectName, denied.Role, denied.Role,
		denied.Privilege, denied.Role, strings.Join(dependents, ", "),
	)
}

// dependents names, sorted and without repeats, the grantee of each grant the
// diff keeps that the grantee of denied made of the permission where denied
// reaches: the grants its CASCADE takes.
func (d denies) dependents(
	diff *difftypes.SchemaDiff, denied difftypes.GrantRef, kept func(difftypes.GrantRef) bool,
) []string {
	want := d.key(denied)
	var dependents []string
	for _, held := range diff.CurrentGrants {
		key := d.key(held)
		if held.GrantedBy != denied.Role || held.Role == denied.Role || key.privilege != want.privilege ||
			key.objectType != want.objectType || key.object != want.object || !want.reaches(key) || !kept(held) {
			continue
		}
		dependents = append(dependents, grantee(held))
	}
	slices.Sort(dependents)
	return slices.Compact(dependents)
}

// columnGrantsDeniesTake is the grant of each column permission a DENY on the
// whole object takes and the schema declares
// ([difftypes.SchemaDiff.GrantsDeniesTake]), which the plan grants again
// after the DENY. The server keeps a column grant written after a DENY on its
// object, so the schema that holds both reads back as itself. A column grant
// the schema does not declare stays taken, as a DENY run by hand takes it.
func (p *Planner) columnGrantsDeniesTake(diff *difftypes.SchemaDiff) []difftypes.GrantRef {
	if len(diff.GrantsDeniesTake) == 0 {
		return nil
	}
	dropped := p.droppedObjects(diff)
	return slices.DeleteFunc(slices.Clone(diff.GrantsDeniesTake), dropped.holds)
}

// columnDeniesGrantsTake is the DENY of each column permission the database
// holds and the diff keeps that a GRANT or a REVOKE the diff plans on the
// whole object takes, split by where the plan denies it again: afterRevokes
// right after the revokes that take it, afterGrants after the grants. A DENY
// on a column after a grant on its object keeps both rows, and the read
// reports both, so a DENY the database holds stays whatever the plan grants
// or revokes beside it. Without them a plan that grants SELECT on a table
// gives a column back that an administrator denied, and the next plan reads
// as synced. A revoke of the grant option alone keeps the DENY row and needs
// none.
//
// Each DENY carries CASCADE, or is refused, as one the diff adds is
// ([denies.cascades]): a GRANT ... WITH GRANT OPTION the plan runs before it
// leaves the grantee holding the permission grantable.
//
// A DENY the diff adds is left to [Planner.planDenies], which runs it after
// the grants, so it runs once. A rollback adds such a DENY: the reversal of
// `DENY SELECT ON t`, run for a grantee that held SELECT on t and a DENY on
// t.a, grants SELECT on t again and denies t.a again, and the read it plans
// from holds the DENY on t.a.
func (p *Planner) columnDeniesGrantsTake(diff *difftypes.SchemaDiff) (afterRevokes, afterGrants []ast.Node, err error) {
	if !p.deniesOnObjects() || len(diff.CurrentPartialRevokes) == 0 {
		return nil, nil, nil
	}
	d := p.newDenies(diff)
	dropped := p.droppedObjects(diff)
	granted := p.grantsBeforeColumnDenies(diff)
	takes := func(statements []difftypes.GrantRef, denied deniedKey) bool {
		return slices.ContainsFunc(statements, func(statement difftypes.GrantRef) bool {
			key := d.key(statement)
			key.privilege = denied.privilege
			return statement.Column == "" && !dropped.holds(statement) && key == denied.onWholeObject() &&
				(strings.EqualFold(strings.TrimSpace(statement.Privilege), denied.privilege) || strings.EqualFold(strings.TrimSpace(statement.Privilege), "ALL"))
		})
	}
	for _, held := range diff.CurrentPartialRevokes {
		if !held.Denied || held.Column == "" || dropped.holds(held) || removes(diff.DeniesRemoved, held) ||
			removes(diff.DeniesAdded, held) {
			continue
		}
		key := d.key(held)
		switch {
		case takes(diff.GrantsRemoved, key):
			node, err := d.deny(diff, held, dropped, nil)
			if err != nil {
				return nil, nil, err
			}
			afterRevokes = append(afterRevokes, node)
		case takes(diff.GrantsAdded, key) || takes(diff.GrantOptionsAdded, key):
			node, err := d.deny(diff, held, dropped, granted)
			if err != nil {
				return nil, nil, err
			}
			afterGrants = append(afterGrants, node)
		}
	}
	return afterRevokes, afterGrants, nil
}

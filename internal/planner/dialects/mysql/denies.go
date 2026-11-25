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
//   - a DENY of a permission the grantee holds WITH GRANT OPTION on the
//     securable, or on one of the columns a DENY on the whole object reaches,
//     answers Msg 4611 without CASCADE.
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
		role:       strings.TrimSpace(ref.Role),
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

// revokeDenies emits a REVOKE for each DENY the diff takes away. It runs with
// the other revokes, at the start of the plan; a REVOKE of a DENY on the whole
// object takes the permission's column rows too, and the column grants the
// diff keeps come back after them ([columnGrantsRevokedWithTheirTable]). A
// DENY on an object dropped holds goes with the object.
func (p *Planner) revokeDenies(result []ast.Node, diff *difftypes.SchemaDiff, dropped droppedObjects) []ast.Node {
	for _, denied := range diff.DeniesRemoved {
		if dropped.holds(denied) {
			continue
		}
		result = append(result, ast.NewRevokePrivilege(denied.Role, denied.ObjectType, denied.ObjectName, []string{denied.Privilege}).
			SetColumns(grantColumns(denied)))
	}
	return result
}

// planDenies is the DENY of each permission the diff adds, split by where the
// plan writes it. onObjects runs before the grants the plan adds, and
// onColumns after them: a DENY on a whole object takes the grants on its
// columns, and a GRANT on a whole object takes the denies on its columns, so
// each statement goes where the server keeps both rows. A schema read in
// statement order folds them the same way.
//
// A DENY of a permission the grantee holds WITH GRANT OPTION after the revokes
// carries CASCADE, which the server requires. CASCADE also takes the
// permission from whoever the grantee granted it to, so a plan where someone
// holds it from the grantee and the diff does not revoke it is refused.
func (p *Planner) planDenies(diff *difftypes.SchemaDiff) (onObjects, onColumns []ast.Node, err error) {
	if len(diff.DeniesAdded) == 0 {
		return nil, nil, nil
	}
	d := p.newDenies(diff)
	dropped := p.droppedObjects(diff)
	for _, denied := range diff.DeniesAdded {
		cascade, err := d.cascades(diff, denied, dropped)
		if err != nil {
			return nil, nil, err
		}
		node := ast.NewDenyPrivilege(denied.Role, denied.ObjectType, denied.ObjectName, []string{denied.Privilege}).
			SetColumns(grantColumns(denied)).
			SetCascade(cascade)
		if denied.Column == "" {
			onObjects = append(onObjects, node)
			continue
		}
		onColumns = append(onColumns, node)
	}
	return onObjects, onColumns, nil
}

// cascades reports whether the DENY of denied needs CASCADE: the grantee
// holds the permission WITH GRANT OPTION on the securable once the revokes
// have run, on the whole object or on a column the DENY reaches. It refuses
// the DENY where someone holds the permission from the grantee and the diff
// keeps it.
func (d denies) cascades(diff *difftypes.SchemaDiff, denied difftypes.GrantRef, dropped droppedObjects) (bool, error) {
	want := d.key(denied)
	kept := func(held difftypes.GrantRef) bool {
		return !dropped.holds(held) && !removes(diff.GrantsRemoved, held)
	}
	grantable := slices.ContainsFunc(diff.CurrentGrants, func(held difftypes.GrantRef) bool {
		key := d.key(held)
		reached := key == want || key.onWholeObject() == want || (want.column != "" && key == want.onWholeObject())
		return held.WithOption && reached && kept(held) && !removes(diff.GrantOptionsRevoked, held)
	})
	if !grantable {
		return false, nil
	}
	var dependents []string
	for _, held := range diff.CurrentGrants {
		key := d.key(held)
		if held.GrantedBy != denied.Role || held.Role == denied.Role || key.privilege != want.privilege ||
			key.objectType != want.objectType || key.object != want.object || !kept(held) {
			continue
		}
		dependents = append(dependents, grantee(held))
	}
	if len(dependents) == 0 {
		return true, nil
	}
	slices.Sort(dependents)
	return false, fmt.Errorf(
		"%w: sqlserver: cannot deny %s on %s to %q: %q holds it WITH GRANT OPTION, so SQL Server denies it with "+
			"CASCADE, which also takes the %s that %q granted to %s, and the schema does not revoke those grants; "+
			"revoke them first, or declare their REVOKE in the schema",
		ptaherr.ErrUnsupportedFeature, denied.Privilege, denied.ObjectName, denied.Role, denied.Role,
		denied.Privilege, denied.Role, strings.Join(slices.Compact(dependents), ", "),
	)
}

// columnGrantsDeniesTake is the grant of each column permission a DENY on the
// whole object takes and the diff keeps, which the plan grants again after the
// DENY. The server keeps a column grant written after a DENY on its object, so
// the schema that holds both reads back as itself.
func (p *Planner) columnGrantsDeniesTake(diff *difftypes.SchemaDiff) []difftypes.GrantRef {
	d := p.newDenies(diff)
	onObjects := make(map[deniedKey]bool)
	for _, denied := range diff.DeniesAdded {
		if denied.Column == "" {
			onObjects[d.key(denied)] = true
		}
	}
	if len(onObjects) == 0 {
		return nil
	}
	dropped := p.droppedObjects(diff)
	var taken []difftypes.GrantRef
	for _, held := range diff.CurrentGrants {
		if held.Column == "" || !onObjects[d.key(held).onWholeObject()] || dropped.holds(held) || removes(diff.GrantsRemoved, held) {
			continue
		}
		held.WithOption = held.WithOption && !removes(diff.GrantOptionsRevoked, held)
		taken = append(taken, held)
	}
	return taken
}

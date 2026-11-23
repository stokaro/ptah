package mysql

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// droppedObjects keys the tables, views, materialized views, sequences and
// routines a plan drops, the way the target compares the object a grant names.
// The zero value holds nothing.
//
// It keys every kind by its name alone. That is sound where it is read, on
// Oracle, which keeps tables, views, sequences and routines in one namespace per
// schema and reports a grant on any of them under its bare name.
type droppedObjects struct {
	semantics identifier.Semantics
	names     map[string]bool
}

// droppedObjects reads the objects diff drops: those it removes, and each
// changed routine the plan replaces by DROP and CREATE
// ([Planner.replacedRoutines]).
func (p *Planner) droppedObjects(diff *difftypes.SchemaDiff) droppedObjects {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	dropped := droppedObjects{semantics: semantics, names: make(map[string]bool)}
	for key := range p.replacedRoutines(diff, semantics) {
		dropped.names[key] = true
	}
	for _, name := range diff.TablesRemoved.Names() {
		dropped.add(name)
	}
	for _, view := range diff.ViewsRemoved {
		dropped.add(view.Name)
	}
	for _, view := range diff.MaterializedViewsRemoved {
		dropped.add(view.Name)
	}
	for _, sequence := range diff.SequencesRemoved {
		dropped.add(sequence.QualifiedName())
	}
	for _, routine := range slices.Concat(diff.FunctionsRemoved, diff.ProceduresRemoved) {
		dropped.add(routine.Name)
	}
	return dropped
}

// replacedRoutines is the declaration of each changed routine diff replaces
// by DROP and CREATE, keyed the way semantics compares the object a grant
// names. [Planner.replacesRoutine] decides it for both the statements and the
// privileges: the revokes [Planner.droppedObjects] leaves out and the grants
// [Planner.heldOnReplacedRoutines] gives back.
func (p *Planner) replacedRoutines(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[string]schemamodel.Function {
	replaced := make(map[string]schemamodel.Function)
	for _, change := range diff.FunctionsModified {
		if p.replacesRoutine(change) {
			replaced[semantics.QualifiedTableIdentityKey(change.Desired.Name)] = change.Desired
		}
	}
	return replaced
}

// heldGrant is a privilege the read reported on a routine the plan replaces,
// with WithOption set where the grant option comes back with it.
type heldGrant struct {
	grant   difftypes.GrantRef
	routine schemamodel.Function
}

// heldOnReplacedRoutines is every privilege the read reported on a routine
// the plan replaces by DROP and CREATE, except the ones the plan revokes,
// which it leaves revoked. A privilege held WITH GRANT OPTION keeps the option
// unless the plan revokes the option. [Planner.grantsGivenBack] grants them
// after the CREATE, and [Planner.refuseALostGrantOption] refuses a plan that
// would have to give back an option the target's renderer does not write.
//
// The DROP takes every privilege on the routine, and the CREATE brings back
// none: measured on Oracle 23.26.3 and SQL Server 2025, and on MySQL 8.4 and
// MariaDB 11.8, where DROP PROCEDURE deletes the routine's rows of
// mysql.procs_priv. The comparison reads the privileges as held and plans
// nothing for them, so without these a body change leaves a role nothing on
// the routine until a later apply grants the declared ones again.
//
// They are what the read reported, not what the declaration grants: the
// comparison leaves alone a privilege of a grantee the schema does not manage,
// and the drop takes it all the same. So they are only as wide as the read,
// which on Oracle and SQL Server reports every grantee of a privilege on the
// schema's objects: a role, a user and PUBLIC. A SQL Server DENY on the
// routine is not a privilege held, and [Planner.deniesGivenBack] writes it
// again.
//
// They are exact for a plan applied to the database it read. A versioned
// migration is planned against the database its directory replays onto, so
// a privilege granted only on the database the migration later runs on is
// not given back.
//
// Only a target that grants on a routine by its name
// ([descriptor.AllPrivileges.RoutinesByName]) has privileges to give back:
// SQL Server and Oracle, whose reads report one under TABLE, the way such a
// GRANT names it. A MySQL or MariaDB read reports no privilege on a routine,
// and a table privilege there cannot be a routine's, since a routine is
// granted on with ON FUNCTION or ON PROCEDURE. A target without
// [capability.RoleManagement] plans no grant at all.
func (p *Planner) heldOnReplacedRoutines(diff *difftypes.SchemaDiff) []heldGrant {
	if !p.capabilities().Has(capability.RoleManagement) || !routinesByName.Get(p.targetDialect()) {
		return nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	replaced := p.replacedRoutines(diff, semantics)
	if len(replaced) == 0 {
		return nil
	}
	revoked := make(map[givenBackKey]bool)
	for _, grant := range diff.GrantsRemoved {
		revoked[givenBackKeyOf(semantics, grant)] = true
	}
	optionRevoked := make(map[givenBackKey]bool)
	for _, grant := range diff.GrantOptionsRevoked {
		optionRevoked[givenBackKeyOf(semantics, grant)] = true
	}
	var held []heldGrant
	for _, grant := range diff.CurrentGrants {
		key := givenBackKeyOf(semantics, grant)
		routine, onReplaced := replaced[key.object]
		if !strings.EqualFold(grant.ObjectType, "TABLE") || !onReplaced || revoked[key] {
			continue
		}
		grant.WithOption = grant.WithOption && !optionRevoked[key]
		held = append(held, heldGrant{grant: grant, routine: routine})
	}
	return held
}

// grantsGivenBack is the GRANT, after the CREATE, of each privilege
// [Planner.heldOnReplacedRoutines] reports, followed by the DENY of each one
// [Planner.deniesGivenBack] reports. The grants the plan adds are written
// after these, as on any other object, so a declared GRANT of a privilege
// the grantee was denied replaces the DENY, as it does on a routine the plan
// keeps.
func (p *Planner) grantsGivenBack(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	held := p.heldOnReplacedRoutines(diff)
	nodes := make([]ast.Node, 0, len(held))
	for _, given := range held {
		grant := given.grant
		nodes = append(nodes, ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(grant.WithOption))
	}
	denies, err := p.deniesGivenBack(diff)
	if err != nil {
		return nil, err
	}
	return append(nodes, denies...), nil
}

// deniesGivenBack is the DENY, after the CREATE, of each privilege the read
// reported denied on a routine the plan replaces by DROP and CREATE, to
// whichever grantee it was denied to.
//
// The DROP takes a DENY with the routine, measured on SQL Server 2025, and the
// comparison plans none, because Ptah declares no DENY. Without these a body
// change widens access in silence: a role granted EXECUTE on the schema and
// denied it on the routine runs the routine after the apply, and the next plan
// reports the schema synced.
//
// Only a target whose partial revoke on an object is a DENY
// ([Planner.deniesOnObjects]) has one to write. A plan the comparison runs
// revokes no DENY: a REVOKE of a privilege the grantee is denied is a
// declared absence, which the DENY keeps. A privilege name is written as the
// catalog spells it, and one that is not a run of words is refused rather
// than written into the statement.
func (p *Planner) deniesGivenBack(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if !p.deniesOnObjects() || !p.capabilities().Has(capability.RoleManagement) || len(diff.CurrentPartialRevokes) == 0 {
		return nil, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	replaced := p.replacedRoutines(diff, semantics)
	var nodes []ast.Node
	for _, denied := range diff.CurrentPartialRevokes {
		_, onReplaced := replaced[semantics.QualifiedTableIdentityKey(denied.ObjectName)]
		if !strings.EqualFold(denied.ObjectType, "TABLE") || denied.Column != "" || !onReplaced {
			continue
		}
		statement, err := denyStatement(denied)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, ast.NewRawSQL(statement))
	}
	return nodes, nil
}

// denyStatement spells denied as T-SQL. The object is named as the read
// qualifies it, each part bracketed, which is how the renderer names the
// object of the GRANT beside it.
func denyStatement(denied difftypes.GrantRef) (string, error) {
	if !permissionName.MatchString(denied.Privilege) {
		return "", fmt.Errorf("%w: sqlserver: cannot deny %q on %s to %q again after the routine is replaced: "+
			"the catalog names a permission Ptah does not write", ptaherr.ErrUnsupportedFeature,
			denied.Privilege, denied.ObjectName, denied.Role)
	}
	ref, ok := tableref.Parse(denied.ObjectName)
	if !ok {
		return "", fmt.Errorf("%w: sqlserver: cannot deny %s to %q again after the routine is replaced: "+
			"%q is not an object name", ptaherr.ErrInvalidSchemaDiff, denied.Privilege, denied.Role, denied.ObjectName)
	}
	object := sqlident.Quote(platform.SQLServer, ref.Name)
	if ref.Qualified {
		object = sqlident.Quote(platform.SQLServer, ref.Schema) + "." + object
	}
	return fmt.Sprintf("DENY %s ON %s TO %s", denied.Privilege, object, sqlident.Quote(platform.SQLServer, denied.Role)), nil
}

// permissionName is the shape of a permission sys.database_permissions
// names: upper-case words, such as EXECUTE or VIEW DEFINITION.
var permissionName = regexp.MustCompile(`^[A-Z]+( [A-Z]+)*$`)

// refuseALostGrantOption refuses a plan that replaces a routine on which a
// grantee holds a privilege WITH GRANT OPTION, where the target's renderer
// writes no grant option ([Planner.writesGrantOption]). The DROP would take
// the option, and the give-back could not write it, so the apply would leave
// the grantee the privilege without the option it held. Oracle is that target,
// and there CREATE OR REPLACE alone keeps the option, measured on 23.26.3, so
// the refusal names it as the way round.
func (p *Planner) refuseALostGrantOption(diff *difftypes.SchemaDiff) error {
	if p.writesGrantOption() {
		return nil
	}
	for _, held := range p.heldOnReplacedRoutines(diff) {
		if !held.grant.WithOption {
			continue
		}
		return fmt.Errorf(
			"%w: %s: cannot replace %s %s: grantee %q holds %s on it WITH GRANT OPTION, and the DROP that "+
				"replaces the routine takes the option, which Ptah cannot grant back; revoke the option, or "+
				"change the routine by hand with CREATE OR REPLACE, which keeps it",
			ptaherr.ErrUnsupportedFeature, p.targetDialect(), routineWord(held.routine), held.routine.Name,
			held.grant.Role, held.grant.Privilege,
		)
	}
	return nil
}

// routinesByName holds each descriptor's AllPrivileges.RoutinesByName.
var routinesByName = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.AllPrivileges.RoutinesByName
})

// givenBackKey is one privilege of one role on one object, the object keyed as
// the target compares it. The revokes and the held grants it matches come from
// the same read, which spells the role and the privilege one way.
type givenBackKey struct {
	role, privilege, object string
}

func givenBackKeyOf(semantics identifier.Semantics, grant difftypes.GrantRef) givenBackKey {
	return givenBackKey{
		role:      grant.Role,
		privilege: grant.Privilege,
		object:    semantics.QualifiedTableIdentityKey(grant.ObjectName),
	}
}

func (d droppedObjects) add(name string) {
	d.names[d.semantics.QualifiedTableIdentityKey(name)] = true
}

// holds reports whether grant is on an object the plan drops, or on a column
// of one.
func (d droppedObjects) holds(grant difftypes.GrantRef) bool {
	return len(d.names) > 0 && d.names[d.semantics.QualifiedTableIdentityKey(grant.ObjectName)]
}

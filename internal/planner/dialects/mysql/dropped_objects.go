package mysql

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/catalog"
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
// [Planner.heldOnRecreatedRoutines] gives back.
func (p *Planner) replacedRoutines(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[string]schemamodel.Function {
	replaced := make(map[string]schemamodel.Function)
	for _, change := range diff.FunctionsModified {
		if p.replacesRoutine(change) {
			replaced[semantics.QualifiedTableIdentityKey(change.Desired.Name)] = change.Desired
		}
	}
	return replaced
}

// heldGrant is a privilege the read reported on a routine the plan recreates,
// with WithOption set where the grant option comes back with it.
type heldGrant struct {
	grant    difftypes.GrantRef
	restored bool
}

// heldOnRecreatedRoutines is every privilege the read reported on a routine
// the plan creates again, after a replacement or in a rollback, except the
// ones the plan revokes, which it leaves revoked. A privilege held WITH GRANT
// OPTION keeps the option unless the plan revokes it.
// [Planner.grantsGivenBack] grants them after the CREATE.
// Restored additions may copy an observed grant option through
// [Planner.restoredGrantOption].
//
// The DROP takes every privilege on the routine, and the CREATE brings back
// none: measured on Oracle 23.26.3 and SQL Server 2025, and on MySQL 8.4 and
// MariaDB 11.8, where DROP PROCEDURE deletes the routine's rows of
// mysql.procs_priv. The comparison reads the privileges as held and plans
// nothing for them, so a routine created again after a DROP needs these
// grants even when the comparison reports no change to them.
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
// GRANT names it. A table privilege on MySQL or MariaDB cannot be a
// routine's, since a routine is granted on with ON FUNCTION or ON PROCEDURE;
// [Planner.routinePrivilegesGivenBack] gives back what the read found there.
// A target without [capability.RoleManagement] plans no grant at all.
func (p *Planner) heldOnRecreatedRoutines(diff *difftypes.SchemaDiff) []heldGrant {
	if !p.capabilities().Has(capability.RoleManagement) || !routinesByName.Get(p.targetDialect()) {
		return nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	replaced := p.recreatedRoutines(diff, semantics)
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
		held = append(held, heldGrant{grant: grant,
			restored: addedRoutine(diff, semantics, routine.Name)})
	}
	return held
}

// grantsGivenBack is the GRANT, after the CREATE, of each privilege
// [Planner.heldOnRecreatedRoutines] reports, followed by the DENY of each one
// [Planner.deniesGivenBack] reports and the GRANT of each one
// [Planner.routinePrivilegesGivenBack] reports. The grants the plan adds are
// written after these, as on any other object, so a declared GRANT of a
// privilege the grantee was denied replaces the DENY, as it does on a routine
// the plan keeps.
func (p *Planner) grantsGivenBack(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	held := p.heldOnRecreatedRoutines(diff)
	nodes := make([]ast.Node, 0, len(held))
	for _, given := range held {
		grant := given.grant
		if given.restored && grant.WithOption && !p.writesGrantOption() {
			node, err := p.restoredGrantOption(grant)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
			continue
		}
		nodes = append(nodes, ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(grant.WithOption))
	}
	denies, err := p.deniesGivenBack(diff)
	if err != nil {
		return nil, err
	}
	privileges, err := p.routinePrivilegesGivenBack(diff)
	if err != nil {
		return nil, err
	}
	return slices.Concat(nodes, denies, privileges), nil
}

// routinePrivilegesGivenBack is the GRANT, after the CREATE, of what each
// account held on a MySQL or MariaDB routine the plan creates again after a
// replacement or in a rollback, as the read found it in mysql.procs_priv
// ([difftypes.SchemaDiff.CurrentRoutinePrivileges]).
//
// Under the default automatic_sp_privileges the DROP deletes every row of
// mysql.procs_priv for the routine, measured on MySQL 8.4.13 and MariaDB
// 11.8.9, and the comparison reads none of them, so without these a body
// change leaves every grantee without EXECUTE on the routine and the next
// plan reports the schema synced. With automatic_sp_privileges off the DROP
// keeps the rows and the read reports none, so the plan grants nothing. Ptah
// models no privilege on a routine on these engines, so the plan revokes none
// and every row comes back, to a user or a role alike: a user is named with
// its host, a MariaDB role by its name alone ([catalog.RoutineGrant.Role]).
// The row the CREATE grants the applying account is not among them.
//
// The GRANTs run as the applying account, and a server refuses one from an
// account without the grant option, with ERROR 1370 after the DROP and the
// CREATE have run. So a row the account may not grant again
// ([catalog.RoutineGrant.ReaderMayGrant]) refuses the plan before anything
// runs.
//
// A routine name compares without case on these engines, and a row spells the
// name as the GRANT that wrote it did, so the name is compared without case
// and the GRANT names the routine as the CREATE does, with its database only
// where the CREATE names one. A routine named without its database is in the
// one database the read describes, which is the connection's, and naming that
// database would tie a generated migration to the database it was planned
// against. A privilege name is written as the server spells it, and one that
// is not a run of words is refused rather than written into the statement.
func (p *Planner) routinePrivilegesGivenBack(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if !p.isMySQLFamily() || len(diff.CurrentRoutinePrivileges.Grants) == 0 {
		return nil, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	var nodes []ast.Node
	for _, routine := range p.recreatedRoutineList(diff) {
		ref, parsed := tableref.Parse(routine.Name)
		if !parsed {
			ref = tableref.Ref{Name: routine.Name}
		}
		for _, grant := range diff.CurrentRoutinePrivileges.Grants {
			if !grantsOn(semantics, grant, routine, ref) {
				continue
			}
			if !grant.ReaderMayGrant {
				return nil, p.refuseUngrantableRoutinePrivilege(grant, routine)
			}
			statement, err := p.routineGrantStatement(grant, routine, ref)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, ast.NewRawSQL(statement))
		}
	}
	return nodes, nil
}

// replacedRoutineList is each routine diff replaces by DROP and CREATE
// ([Planner.replacesRoutine]), in the order the diff changes them.
func (p *Planner) replacedRoutineList(diff *difftypes.SchemaDiff) []schemamodel.Function {
	var replaced []schemamodel.Function
	for _, change := range diff.FunctionsModified {
		if p.replacesRoutine(change) {
			replaced = append(replaced, change.Desired)
		}
	}
	return replaced
}

// grantsOn reports whether grant is a privilege on routine, whose name ref
// parses.
func grantsOn(semantics identifier.Semantics, grant catalog.RoutineGrant, routine schemamodel.Function, ref tableref.Ref) bool {
	if !strings.EqualFold(grant.Type, routineWord(routine)) || !strings.EqualFold(grant.Name, ref.Name) {
		return false
	}
	return !ref.Qualified || semantics.TableIdentityKey(ref.Schema) == semantics.TableIdentityKey(grant.Schema)
}

// routineGrantStatement spells the GRANT that gives grant back on routine,
// whose name ref parses: qualified where the routine's name is, which is how
// the DROP and the CREATE name it.
func (p *Planner) routineGrantStatement(grant catalog.RoutineGrant, routine schemamodel.Function, ref tableref.Ref) (string, error) {
	privileges := routineGrantPrivileges(grant)
	if !permissionName.MatchString(strings.ReplaceAll(privileges, ", ", " ")) {
		return "", fmt.Errorf("%w: %s: cannot grant %q on %s %s to %q again after the routine is replaced: "+
			"the catalog names a privilege Ptah does not write", ptaherr.ErrUnsupportedFeature, p.targetDialect(),
			privileges, routineWord(routine), routine.Name, grant.User)
	}
	dialect := p.targetDialect()
	name := sqlident.Quote(dialect, ref.Name)
	if ref.Qualified {
		name = sqlident.Quote(dialect, ref.Schema) + "." + name
	}
	statement := fmt.Sprintf("GRANT %s ON %s %s TO %s", privileges, strings.ToUpper(routineWord(routine)), name,
		p.routineGrantee(grant))
	if grant.WithOption {
		statement += " WITH GRANT OPTION"
	}
	return statement, nil
}

// routineGrantPrivileges is the privilege list of the GRANT that gives grant
// back: USAGE for a row that holds only the grant option.
func routineGrantPrivileges(grant catalog.RoutineGrant) string {
	if len(grant.Privileges) == 0 {
		return "USAGE"
	}
	return strings.Join(grant.Privileges, ", ")
}

// routineGrantee names the account of grant as a GRANT names it.
func (p *Planner) routineGrantee(grant catalog.RoutineGrant) string {
	grantee := sqlident.Quote(p.targetDialect(), grant.User)
	if !grant.Role {
		grantee += "@" + sqlident.Quote(p.targetDialect(), grant.Host)
	}
	return grantee
}

// refuseUngrantableRoutinePrivilege refuses the plan that replaces routine,
// on which grant is held and the applying account may not grant it again.
// The GRANT would fail with ERROR 1370 after the DROP and the CREATE have run,
// measured on MySQL 8.4.13 and MariaDB 11.8.9, and the grantee would be left
// without the privilege.
func (p *Planner) refuseUngrantableRoutinePrivilege(grant catalog.RoutineGrant, routine schemamodel.Function) error {
	privileges := routineGrantPrivileges(grant)
	held := privileges
	if len(grant.Privileges) == 0 {
		held = "the grant option"
	}
	database := sqlident.Quote(p.targetDialect(), grant.Schema) + ".*"
	return fmt.Errorf(
		"%w: %s: cannot replace %s %s: the DROP that replaces it takes %s on it from %s, and the account may not "+
			"grant it again without the grant option on %s; grant the account %s ON %s WITH GRANT OPTION, or "+
			"replace the routine as an account that holds it",
		ptaherr.ErrUnsupportedFeature, p.targetDialect(), routineWord(routine), routine.Name, held,
		p.routineGrantee(grant), database, privileges, database,
	)
}

// refuseUnreadRoutinePrivileges refuses a plan that replaces a MySQL or
// MariaDB routine by DROP and CREATE where the server refused the read of
// mysql.procs_priv ([catalog.RoutinePrivileges.Refused]). The DROP takes
// every privilege on the routine, and nothing says which there were, so the
// apply would take them without a word.
func (p *Planner) refuseUnreadRoutinePrivileges(diff *difftypes.SchemaDiff) error {
	if !p.isMySQLFamily() || !diff.CurrentRoutinePrivileges.Refused {
		return nil
	}
	replaced := p.replacedRoutineList(diff)
	if len(replaced) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: %s: cannot replace %s %s: the account may not read mysql.procs_priv, so nothing says who "+
			"holds privileges on the routine, and the DROP that replaces it takes them all; grant the "+
			"account SELECT on mysql.procs_priv",
		ptaherr.ErrUnsupportedFeature, p.targetDialect(), routineWord(replaced[0]), replaced[0].Name,
	)
}

// deniesGivenBack is the DENY, after the CREATE, of each privilege the read
// reported denied on a routine the plan creates again, to whichever grantee
// it was denied to.
//
// The DROP takes a DENY with the routine, measured on SQL Server 2025, and the
// comparison plans no DENY the database already holds. Without these a body
// change widens access in silence: a role granted EXECUTE on the schema and
// denied it on the routine runs the routine after the apply, and the next plan
// reports the schema synced.
//
// Only a target whose partial revoke on an object is a DENY
// ([Planner.deniesOnObjects]) has one to write. A DENY the diff takes away
// ([difftypes.SchemaDiff.DeniesRemoved]) is revoked at the start of the plan
// and does not come back; a REVOKE the schema declares for a privilege the
// grantee is denied is a declared absence, which the DENY keeps. A privilege
// name is written as the catalog spells it, and one that is not a run of words
// is refused rather than written into the statement.
func (p *Planner) deniesGivenBack(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if !p.deniesOnObjects() || !p.capabilities().Has(capability.RoleManagement) || len(diff.CurrentPartialRevokes) == 0 {
		return nil, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	replaced := p.recreatedRoutines(diff, semantics)
	var nodes []ast.Node
	for _, denied := range diff.CurrentPartialRevokes {
		_, onReplaced := replaced[semantics.QualifiedTableIdentityKey(denied.ObjectName)]
		if !strings.EqualFold(denied.ObjectType, "TABLE") || denied.Column != "" || !onReplaced || removes(diff.DeniesRemoved, denied) {
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

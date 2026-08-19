package schemastate

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// Policy is a row-level-security policy.
//
// Its name is scoped to the table that carries it, which is the whole reason it
// is here: `CREATE POLICY tenant_isolation` succeeds once on each of two tables,
// so a model keyed on the policy name alone collapses them
// (stokaro/ptah#1276), and one that joins the table and the policy with a dot
// collides whenever either contains one (stokaro/ptah#1311). Both facts are
// already true in [objectidentity]; carrying the policy here is what makes the
// canonical state hold them (stokaro/ptah#1664).
type Policy struct {
	Table      objectidentity.ID
	Command    string
	Role       string
	Using      string
	WithCheck  string
	Permissive bool
}

// Grant is one privilege held by one role on one object.
//
// A grant is a triple, and the triple is the identity. Keying it on a
// delimiter-joined string collapsed two distinct grants (stokaro/ptah#1283),
// which is why the object and the role are carried as their own components
// rather than folded into a name.
type Grant struct {
	Role      string
	Privilege string
	Object    objectidentity.ID
	WithGrant bool
}

// grantObjectTypeSchema is the object type whose target is a schema rather than
// an object inside one.
const grantObjectTypeSchema = "SCHEMA"

// PoliciesFromDescription adds the row-level-security policies a schema
// declares.
//
// The owning table is resolved through the identity model rather than kept as
// the string the source wrote, so a policy on `"tenant.data"` belongs to that
// table and not to a table `data` in a schema `tenant`.
func PoliciesFromDescription(state *State, description *goschema.Database, builder objectidentity.Builder) error {
	tablesByStruct := make(map[string]goschema.Table)
	for _, table := range description.Tables {
		tablesByStruct[table.StructName] = table
	}
	for _, policy := range description.RLSPolicies {
		owner, resolved := resolvePolicyTable(policy, tablesByStruct, builder)
		if !resolved {
			return fmt.Errorf(
				"policy %q names table %q, which this description does not declare",
				policy.Name, policy.Table)
		}
		id := builder.PolicyParts(owner.Schema.Source, owner.Name.Source, policy.Name)
		if existing, collided := state.Add(Object{
			ID: id,
			Policy: &Policy{
				Table: owner, Command: policy.PolicyFor, Role: policy.ToRoles,
				Using: policy.UsingExpression, WithCheck: policy.WithCheckExpression,
			},
			Provenance: Provenance{Source: "description", Location: policy.StructName},
		}); collided {
			return fmt.Errorf("description declares two policies with one identity: %s and %s", existing.ID, id)
		}
	}
	return nil
}

// resolvePolicyTable resolves the table a declared policy names, preferring the
// struct it was parsed from and falling back to the written table name.
func resolvePolicyTable(
	policy goschema.RLSPolicy,
	tablesByStruct map[string]goschema.Table,
	builder objectidentity.Builder,
) (objectidentity.ID, bool) {
	if table, ok := tablesByStruct[policy.StructName]; ok {
		return builder.TableParts(table.Schema, table.Name), true
	}
	if strings.TrimSpace(policy.Table) == "" {
		return objectidentity.ID{}, false
	}
	return builder.Table(policy.Table), true
}

// PoliciesFromCatalog adds the policies a catalog reports.
func PoliciesFromCatalog(state *State, schema *dbschematypes.DBSchema, builder objectidentity.Builder) error {
	for _, policy := range schema.RLSPolicies {
		owner := builder.Table(policy.Table)
		id := builder.PolicyParts(owner.Schema.Source, owner.Name.Source, policy.Name)
		if existing, collided := state.Add(Object{
			ID: id,
			Policy: &Policy{
				Table: owner, Command: policy.PolicyFor, Role: policy.ToRoles,
				Using: policy.UsingExpression, WithCheck: policy.WithCheckExpression,
			},
			Provenance: Provenance{Source: "catalog", Location: "pg_policies"},
		}); collided {
			return fmt.Errorf("catalog reports two policies with one identity: %s and %s", existing.ID, id)
		}
	}
	return nil
}

// CoverageFor returns the coverage kind a canonical object family maps onto, or
// false for a family whose absence is never ambiguous.
//
// Tables, columns and constraints are deliberately absent: core/coverage's kind
// list is closed and holds none of them, because a description that does not
// mention a table is saying the table should go. The families listed here are
// the ones where silence and absence are different facts.
func CoverageFor(kind objectidentity.Kind) (coverage.Kind, bool) {
	mapped, ok := map[objectidentity.Kind]coverage.Kind{
		objectidentity.KindPolicy:    coverage.Policy,
		objectidentity.KindDomain:    coverage.Domain,
		objectidentity.KindSequence:  coverage.Sequence,
		objectidentity.KindComposite: coverage.Composite,
		objectidentity.KindRange:     coverage.Range,
		objectidentity.KindExtension: coverage.Extension,
		objectidentity.KindRole:      coverage.Role,
		objectidentity.KindSchema:    coverage.Schema,
	}[kind]
	return mapped, ok
}

// DescribesObject reports whether a state's description claims to describe one
// object, which is the question a removal has to ask before it is planned.
//
// A description that declined a whole family, or this object in it, is silent
// rather than empty, and reading that silence as absence is what turns a
// partial read into a drop (stokaro/ptah#1028).
func DescribesObject(state *State, id objectidentity.ID) bool {
	kind, ok := CoverageFor(id.Kind)
	if !ok {
		return true
	}
	return state.Coverage().DescribesIn(kind, id.Schema.Source, id.Name.Source)
}

// GrantsFromDescription adds the privilege grants a schema declares, one object
// per privilege.
//
// A declaration naming three privileges is three grants, because that is what
// the target holds: `GRANT SELECT, INSERT ON t TO r` is two rows in the
// catalog, and revoking one of them leaves the other. A model carrying the
// three as one object cannot express the state that follows.
func GrantsFromDescription(state *State, description *goschema.Database, builder objectidentity.Builder) error {
	for _, declared := range description.Grants {
		declared.Canonicalize()
		schema, object := grantTarget(declared)
		for _, privilege := range declared.Privileges {
			id := builder.GrantParts(schema, object, declared.Role, privilege)
			if existing, collided := state.Add(Object{
				ID: id,
				Grant: &Grant{
					Role: declared.Role, Privilege: privilege,
					Object: builder.TableParts(schema, object), WithGrant: declared.WithOption,
				},
				Provenance: Provenance{Source: "description", Location: declared.StructName},
			}); collided {
				return fmt.Errorf("description declares two grants with one identity: %s and %s", existing.ID, id)
			}
		}
	}
	return nil
}

// grantTarget splits a declared grant's target into the schema slot and the
// object slot [objectidentity.Builder.GrantParts] expects.
//
// A schema grant takes the schema slot with the object left empty; the other
// object types name something inside a schema the declaration does not
// qualify, so the schema slot stays empty and the identity builder applies the
// target's own default.
func grantTarget(declared goschema.Grant) (schema, object string) {
	if declared.OnSchema != "" {
		return declared.OnSchema, ""
	}
	if declared.OnSequence != "" {
		return "", declared.OnSequence
	}
	return "", declared.OnTable
}

// GrantsFromCatalog adds the privilege grants a catalog reports.
func GrantsFromCatalog(state *State, schema *dbschematypes.DBSchema, builder objectidentity.Builder) error {
	for _, reported := range schema.Grants {
		if reported.IsPartialRevoke {
			// A row that subtracts from a broader grant is not a grant, and
			// treating it as one would plan a REVOKE of something already
			// revoked. The reader keeps it so a validation can see it.
			continue
		}
		target, object := catalogGrantTarget(reported)
		id := builder.GrantParts(target, object, reported.Role, reported.Privilege)
		if existing, collided := state.Add(Object{
			ID: id,
			Grant: &Grant{
				Role:      strings.TrimSpace(reported.Role),
				Privilege: strings.ToUpper(strings.TrimSpace(reported.Privilege)),
				Object:    builder.TableParts(target, object), WithGrant: reported.WithOption,
			},
			Provenance: Provenance{Source: "catalog", Location: "information_schema"},
		}); collided {
			return fmt.Errorf("catalog reports two grants with one identity: %s and %s", existing.ID, id)
		}
	}
	return nil
}

func catalogGrantTarget(reported dbschematypes.DBGrant) (schema, object string) {
	if strings.EqualFold(reported.ObjectType, grantObjectTypeSchema) {
		return reported.ObjectName, ""
	}
	return reported.Schema, reported.ObjectName
}

// RolesFromDescription adds the roles a schema manages as first-class objects.
//
// Roles are READ into the state, not planned. They are here because a grant
// removal has to ask whether Ptah manages the role that holds it, and asking a
// canonical object is what keeps that question from being answered by a side
// map the comparison has to be handed separately.
func RolesFromDescription(state *State, description *goschema.Database, builder objectidentity.Builder) error {
	for _, role := range description.Roles {
		id := builder.Role(role.Name)
		if existing, collided := state.Add(Object{
			ID:         id,
			Provenance: Provenance{Source: "description", Location: role.StructName},
		}); collided {
			return fmt.Errorf("description declares two roles with one identity: %s and %s", existing.ID, id)
		}
	}
	return nil
}

// RolesFromCatalog adds the roles a catalog reports.
func RolesFromCatalog(state *State, schema *dbschematypes.DBSchema, builder objectidentity.Builder) error {
	for _, role := range schema.Roles {
		id := builder.Role(role.Name)
		if existing, collided := state.Add(Object{
			ID:         id,
			Provenance: Provenance{Source: "catalog", Location: "pg_roles"},
		}); collided {
			return fmt.Errorf("catalog reports two roles with one identity: %s and %s", existing.ID, id)
		}
	}
	return nil
}

// Manages reports whether a state carries the named role as an object it owns.
func (s *State) Manages(role string, builder objectidentity.Builder) bool {
	_, found := s.Get(builder.Role(role))
	return found
}

// ManagedRoles returns the roles a description manages as first-class objects.
//
// It is what a grant removal has to consult. A grant held by a role Ptah does
// not manage is not Ptah's to revoke, and its absence from a description is not
// a request to take it away -- the description was never describing that role's
// privileges in the first place.
func ManagedRoles(description *goschema.Database) map[string]bool {
	managed := make(map[string]bool)
	for _, role := range description.Roles {
		managed[strings.TrimSpace(role.Name)] = true
	}
	return managed
}

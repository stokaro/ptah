package atlashclrender

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

// policyBlock holds the validated inputs for one policy block of the
// PostgreSQL row-security owner.
type policyBlock struct {
	table string
	ref   objectidentity.ID
	value *pgpolicy.DesiredPolicy
}

// captureRowSecurity validates every row-level security value the document
// carries before rendering, so a malformed value cannot become an export
// warning. A parsed document claims to describe every policy and every table's
// switches, so coverage a source could not describe is refused rather than
// written as absent.
func (r *renderer) captureRowSecurity() error {
	if err := pgpolicysource.RequireRepresentable(r.db.FeatureCoverage, r.db.FeatureObjects); err != nil {
		return err
	}
	r.tableStates = make(map[string]*pgpolicy.DesiredTableState)
	for _, table := range r.db.Tables {
		state, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](table.Facets, pgpolicy.TableStateKind)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := pgpolicy.ValidateDesiredTableState(state); err != nil {
			return err
		}
		r.tableStates[table.QualifiedName()] = state
	}
	objects, err := r.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.Ref.Kind != objectidentity.Kind(pgpolicy.PolicyKind) {
			continue
		}
		value, ok := object.Value.(*pgpolicy.DesiredPolicy)
		if !ok {
			return fmt.Errorf("%w: HCL requires desired policy values", schemaext.ErrInvalidValue)
		}
		if err := pgpolicy.ValidatePolicyRef(object.Ref); err != nil {
			return err
		}
		if err := pgpolicy.ValidateDesiredPolicy(value); err != nil {
			return err
		}
		r.policies = append(r.policies, policyBlock{table: schemamodel.QualifyTableName(object.Ref.Schema.Authored(), object.Ref.Parent.Source),
			ref: object.Ref, value: value})
	}
	slices.SortFunc(r.policies, func(a, b policyBlock) int {
		return cmp.Or(cmp.Compare(a.table, b.table), cmp.Compare(a.ref.Name.Source, b.ref.Name.Source))
	})
	return nil
}

// renderPolicies writes one policy block per policy of the row-security
// owner, writing each value its declaration states and nothing it leaves out,
// so the document reads back as the same declaration. A role list HCL cannot
// spell leaves the block out with a loss diagnostic.
func (r *renderer) renderPolicies() {
	for _, block := range r.policies {
		path := "rls_policies." + block.table + "." + block.ref.Name.Source
		to, ok := r.policyRoles(block.value.Roles)
		if !ok {
			r.warn(path, "a role named like a role keyword cannot be represented in HCL")
			continue
		}
		if r.omitRefusedBlock(path, blockPolicy, block.ref.Name.Source) {
			continue
		}
		r.linef(`policy %s {`, quote(block.ref.Name.Source))
		r.rawAttr(1, "on", r.tableRef(block.table))
		r.stringAttr(1, "for", string(block.value.Command))
		r.rawAttr(1, "to", to)
		r.stringAttr(1, "as", strings.ToUpper(string(block.value.Composition)))
		r.stringAttr(1, "using", optionalText(block.value.Using))
		r.stringAttr(1, "check", optionalText(block.value.WithCheck))
		r.stringAttr(1, "comment", block.value.Comment)
		r.line("}")
		r.line("")
	}
}

// policyRoles writes a policy's TO list: a keyword as its quoted spelling, a
// role as [renderer.roleTarget] writes a grantee. It reports false for a role
// named like a keyword, which a reader takes for the keyword.
func (r *renderer) policyRoles(roles []pgpolicy.RoleSelector) (string, bool) {
	if roles == nil {
		return "", true
	}
	targets := make([]string, 0, len(roles))
	for _, role := range pgpolicy.CanonicalRoles(roles) {
		if role.Keyword != "" {
			targets = append(targets, quote(string(role.Keyword)))
			continue
		}
		if pgpolicysource.SpellsKeyword(role.Name) {
			return "", false
		}
		targets = append(targets, r.roleTarget(role.Name))
	}
	return "[" + strings.Join(targets, ", ") + "]", true
}

// renderTableState writes a table's row-level security switches as its
// row_security block. A block can only enable, so a table forced without
// being enabled is reported as a loss, and so is a binding to targets.
func (r *renderer) renderTableState(table schemamodel.Table) {
	state, found := r.tableStates[table.QualifiedName()]
	if !found {
		return
	}
	path := "table." + table.QualifiedName() + ".row_security"
	if scope := table.Facets.TargetScope(pgpolicy.TableStateKind); len(scope) > 0 {
		r.warn(path, fmt.Sprintf("dialect scope %q is not represented in HCL", strings.Join(scope, ",")))
	}
	if !state.Enabled {
		if state.Forced {
			r.warn(path, "FORCE ROW LEVEL SECURITY without ENABLE cannot be represented in HCL")
		}
		return
	}
	r.line("  row_security {")
	r.rawAttr(2, "enabled", "true")
	if state.Forced {
		r.rawAttr(2, "enforced", "true")
	}
	r.stringAttr(2, "comment", state.Comment)
	r.line("  }")
}

// optionalText is a clause a declaration may leave out, empty when it does.
func optionalText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

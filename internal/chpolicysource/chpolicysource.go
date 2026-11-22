// Package chpolicysource reads the ClickHouse owner's row policies
// ([chschema.DesiredRowPolicy]) out of the attributes a schema source spells
// them with. The owner's Go directive and its YAML key share it, so a policy
// reads the same from either, and so does the Go export that writes one back.
//
// A ClickHouse row policy is a SELECT filter with its own role selection and
// composition, not PostgreSQL's policy (ADR 0020), so neither source spells it
// as row-level security.
package chpolicysource

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Attributes is a policy as a text-attribute source spells it: every part a
// string, where an empty one is left out. The composition is the source's to
// read, since the directive and the YAML key spell it alike.
type Attributes struct {
	// To is the role selection in [ParseRoles]'s grammar. Empty applies the
	// policy to nobody.
	To string
	// Using is the filter. Empty declares none, which admits every row.
	Using string
	// StructName is the Go struct the declaration belongs to, if any.
	StructName string
}

// Policy reads the attributes as one row policy declaration, refusing a role
// selection [ParseRoles] refuses and a policy
// [chschema.ValidateDesiredRowPolicy] refuses.
func (a Attributes) Policy() (chschema.DesiredRowPolicy, error) {
	roles, err := ParseRoles(a.To)
	if err != nil {
		return chschema.DesiredRowPolicy{}, err
	}
	policy := chschema.DesiredRowPolicy{Roles: roles, StructName: a.StructName}
	if a.Using != "" {
		policy.Filter = new(a.Using)
	}
	if err := chschema.ValidateDesiredRowPolicy(&policy); err != nil {
		return chschema.DesiredRowPolicy{}, err
	}
	return policy, nil
}

// Ref is the identity of the row policy name on table in database. An empty
// database is the connection's.
func Ref(database, table, name string) objectidentity.ID {
	return chschema.RowPolicyRef(database, table, name)
}

// Coverage is what a desired document that can declare row policies claims
// about them: it describes every one, so a comparison drops a policy the
// document leaves out.
func Coverage() (schemaext.Coverage, error) {
	return chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// Collector gathers one document's row policies and refuses two that share an
// identity, naming both. Its zero value is ready to use.
type Collector struct {
	objects schemaext.Objects
	origins map[objectidentity.Key]string
}

// AddPolicy collects one policy. origin names the declaration in a refusal,
// in the source's own terms. targets is the declaration's target scope, which
// the object keeps.
func (c *Collector) AddPolicy(origin string, ref objectidentity.ID, policy chschema.DesiredRowPolicy, targets []string) error {
	if earlier, found := c.origins[ref.Key()]; found {
		return fmt.Errorf("%w: %s and %s both declare row policy %q on table %q; keep one declaration",
			ptaherr.ErrInvalidAttributeValue, earlier, origin, ref.Name.Source, ref.Parent.Source)
	}
	object, err := chschema.DesiredRowPolicyObject(ref, policy)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	object.Targets = slices.Clone(targets)
	objects, err := c.objects.With(object)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	if c.origins == nil {
		c.origins = make(map[objectidentity.Key]string)
	}
	c.origins[ref.Key()] = origin
	c.objects = objects
	return nil
}

// Objects returns the collected policies.
func (c *Collector) Objects() schemaext.Objects { return c.objects }

// RequireDescribed refuses row policy coverage a source format cannot write
// back: a read that could not inspect the row policies, as a whole or one by
// one, such as one by an account that may not read system.row_policies. A
// format that declares row policies claims to describe every one, so an
// export of such a read would declare none, and applying it would drop the
// policies the server holds.
//
// A single policy the read met and could not describe, such as one written ON
// db.* or one on a view, passes: no source can declare it, so an export leaves
// it out, and a comparison of the database keeps it, since the read records it
// the same way. [Undescribed] lists those for the export to name.
func RequireDescribed(coverage schemaext.Coverage) error {
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == chschema.RowPolicyKind && record.Knowledge.Unscoped().State != schemaext.Complete {
			return fmt.Errorf("%w: ClickHouse row policies cannot be exported: the read did not describe them (%s)",
				ptaherr.ErrUnsupportedFeature, record.Knowledge.Unscoped().Reason)
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == chschema.RowPolicyKind && !exportable(record.Knowledge.State) {
			return fmt.Errorf("%w: ClickHouse row policy %s cannot be exported: the read did not describe it (%s)",
				ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}

// exportable reports whether an export can stand on what the read knows of
// one policy: a description, a policy it knows is absent, or one no source
// can declare. Knowledge of the whole model must be complete, since an export
// that cannot see the policies would claim every one and declare none.
func exportable(state schemaext.KnowledgeState) bool {
	return state == schemaext.Complete || state == schemaext.Absent || state == schemaext.Unrepresentable
}

// Undescribed returns the row policies coverage records as met and not
// described, in coverage order: the policies an export leaves out.
func Undescribed(coverage schemaext.Coverage) []schemaext.SubjectCoverage {
	var undescribed []schemaext.SubjectCoverage
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == chschema.RowPolicyKind && record.Knowledge.State == schemaext.Unrepresentable {
			undescribed = append(undescribed, record)
		}
	}
	return undescribed
}

// Package chpolicysource reads the row-level security declarations a schema
// source scopes to ClickHouse as the ClickHouse owner's row policies
// ([chschema.DesiredRowPolicy]). The Go annotation and YAML sources share it,
// so a policy reads the same from either, and so does the Go export that
// writes one back.
//
// A ClickHouse row policy is a SELECT filter with its own role selection and
// composition, not PostgreSQL's policy (ADR 0020). The shared declaration
// spells both, so this package decides which of its attributes ClickHouse
// keeps and refuses, by name, the ones it would accept and discard.
package chpolicysource

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Owns decides whether a row-level security declaration scoped to targets,
// none of them PostgreSQL-family, is a ClickHouse row policy. A scope naming
// ClickHouse only is; one naming no ClickHouse target is not and stays a
// shared declaration. A scope that names ClickHouse beside another target is
// refused: SQL Server's security policy and ClickHouse's row policy are
// different objects, and the refusal says how to split the declaration.
func Owns(targets []string) (bool, error) {
	var clickhouse, other []string
	for _, target := range targets {
		if platform.NormalizeDialect(target) == platform.ClickHouse {
			clickhouse = append(clickhouse, target)
		} else {
			other = append(other, target)
		}
	}
	if len(clickhouse) > 0 && len(other) > 0 {
		return false, fmt.Errorf("%w: row-level security scoped to %s mixes ClickHouse with other targets; "+
			"a ClickHouse row policy and the other targets' policies are different objects, so declare one scoped to %s and another scoped to %s",
			ptaherr.ErrInvalidAttributeValue, strings.Join(targets, ","), strings.Join(clickhouse, ","), strings.Join(other, ","))
	}
	return len(clickhouse) > 0, nil
}

// RefuseSwitches refuses a row-level security enablement scoped to
// ClickHouse. ClickHouse has no table switch: a row policy filters the rows of
// the users it names once it exists, so an enablement would be accepted and
// mean nothing.
func RefuseSwitches(targets []string) error {
	if !slices.ContainsFunc(targets, func(target string) bool { return platform.NormalizeDialect(target) == platform.ClickHouse }) {
		return nil
	}
	return fmt.Errorf("%w: ClickHouse has no row-level security switch: a row policy filters rows once it exists; "+
		"leave clickhouse out of the enablement's dialects", ptaherr.ErrInvalidAttributeValue)
}

// Attributes is a policy as a text-attribute source spells it: every part a
// string, where an empty one is left out.
type Attributes struct {
	// For is the command. ClickHouse filters SELECT only, so it is empty, ALL
	// or SELECT, in any letter case.
	For string
	// To is the role selection in [ParseRoles]'s grammar. Empty applies the
	// policy to nobody.
	To string
	// Using is the filter. Empty declares none, which admits every row.
	Using string
	// WithCheck is a write check, which ClickHouse has no counterpart for.
	WithCheck string
	// Restrictive selects AS RESTRICTIVE.
	Restrictive bool
	// Comment is a comment, which a ClickHouse row policy cannot hold.
	Comment string
	// StructName is the Go struct the declaration belongs to, if any.
	StructName string
}

// Policy reads the attributes as one row policy declaration. An attribute
// ClickHouse would accept and discard is refused, naming it: a write check,
// which ClickHouse parses and drops, so the policy would filter reads and leave
// writes open; a command other than SELECT, which it does not parse; and a
// comment, which it has nowhere to keep.
func (a Attributes) Policy() (chschema.DesiredRowPolicy, error) {
	switch {
	case strings.TrimSpace(a.WithCheck) != "":
		return chschema.DesiredRowPolicy{}, fmt.Errorf("%w: a ClickHouse row policy has no write check: ClickHouse parses WITH CHECK and discards it, "+
			"so the policy would filter reads and leave writes open", ptaherr.ErrInvalidAttributeValue)
	case !slices.Contains([]string{"", "ALL", "SELECT"}, strings.ToUpper(strings.TrimSpace(a.For))):
		return chschema.DesiredRowPolicy{}, fmt.Errorf("%w: a ClickHouse row policy filters SELECT only; FOR %s is not one ClickHouse parses "+
			"(`Expected one of: ALL, SELECT`)", ptaherr.ErrInvalidAttributeValue, strings.ToUpper(strings.TrimSpace(a.For)))
	case a.Comment != "":
		return chschema.DesiredRowPolicy{}, fmt.Errorf("%w: a ClickHouse row policy holds no comment", ptaherr.ErrInvalidAttributeValue)
	}
	roles, err := ParseRoles(a.To)
	if err != nil {
		return chschema.DesiredRowPolicy{}, err
	}
	policy := chschema.DesiredRowPolicy{Roles: roles, StructName: a.StructName}
	if a.Using != "" {
		policy.Filter = new(a.Using)
	}
	if a.Restrictive {
		policy.Composition = chschema.Restrictive
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
// back: knowledge of the model, or of one policy, that is not complete. A
// format that declares row policies claims to describe every one, so an
// export of a read that could not see them would declare none, and applying
// it would drop the policies the server holds.
func RequireDescribed(coverage schemaext.Coverage) error {
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == chschema.RowPolicyKind && record.Knowledge.State != schemaext.Complete {
			return fmt.Errorf("%w: ClickHouse row policies cannot be exported: the read did not describe them (%s)",
				ptaherr.ErrUnsupportedFeature, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == chschema.RowPolicyKind && record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return fmt.Errorf("%w: ClickHouse row policy %s cannot be exported: the read did not describe it (%s)",
				ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}

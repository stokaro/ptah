// Package mssqlpolicysource turns a row-level security declaration scoped to
// SQL Server into a security policy of the owner in package mssqlschema. A
// source decodes the shared declaration grammar, a policy on a table with a
// USING and a WITH CHECK expression, and this package maps it onto the
// predicates SQL Server takes, collects the declarations of one document, and
// says what the document claims to describe.
//
// It sits beside package pgpolicysource, which does the same for PostgreSQL:
// [Owns] decides which of the two, or neither, holds a declaration.
package mssqlpolicysource

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/tableref"
)

// DefaultSchema is the schema SQL Server resolves an unqualified name into
// for a user with no default schema of its own, and the one a predicate's
// table takes when the declaration names none.
const DefaultSchema = "dbo"

// Owns reports whether a declaration scoped to targets is a SQL Server
// security policy: its scope names SQL Server and no other target. A scope
// naming SQL Server beside another target is refused, since a security policy
// and another target's policy are different objects with different grammars,
// and the refusal says how to split it. An empty scope is not SQL Server's.
func Owns(targets []string) (bool, error) {
	var sqlServer, other []string
	for _, target := range targets {
		if platform.NormalizeDialect(target) == platform.SQLServer {
			sqlServer = append(sqlServer, target)
		} else {
			other = append(other, target)
		}
	}
	if len(sqlServer) > 0 && len(other) > 0 {
		return false, fmt.Errorf("%w: row-level security scoped to %s mixes SQL Server with other targets; "+
			"a SQL Server security policy and the other targets' policies are different objects, so declare one scoped to %s and another scoped to %s",
			ptaherr.ErrInvalidAttributeValue, strings.Join(targets, ","), strings.Join(sqlServer, ","), strings.Join(other, ","))
	}
	return len(sqlServer) > 0, nil
}

// Attributes is one row-level security declaration as a source spells it,
// with the table it is on already resolved.
type Attributes struct {
	// Name is the policy's name, qualified by its schema or not. An
	// unqualified policy is created in its table's schema.
	Name string
	// TableSchema and Table name the table the predicates bind. An empty
	// schema is [DefaultSchema].
	TableSchema, Table string
	// For is the command, in any letter case. Empty and ALL cover every
	// operation.
	For string
	// To is the role list, which SQL Server has no clause for.
	To string
	// Using and WithCheck are the filter and the block predicate, each a call
	// of a two-part inline table-valued function, as [mssqlschema.ParseInvocation]
	// reads it. Empty declares no such predicate.
	Using, WithCheck string
	// Restrictive and Comment have no SQL Server form.
	Restrictive bool
	Comment     string
	// StructName is the Go struct the declaration belongs to, if any.
	StructName string
}

// Policy maps the declaration onto a security policy and its identity.
//
// The USING expression becomes a filter predicate and the WITH CHECK
// expression a block predicate, both on the table. A block predicate takes
// the operation FOR names: INSERT is AFTER INSERT, UPDATE is AFTER UPDATE and
// DELETE is BEFORE DELETE, and ALL, or no FOR, covers every write. The policy
// keeps SQL Server's defaults: enabled and schema bound.
//
// What SQL Server cannot hold is refused rather than dropped, with
// [ptaherr.ErrInvalidAttributeValue]: a TO clause, since SQL Server scopes a
// predicate inside its function and a dropped role list applies to everyone;
// FOR SELECT, or FOR INSERT, UPDATE or DELETE without WITH CHECK, since a
// filter predicate has no per-operation form and the policy would cover every
// operation; AS RESTRICTIVE and a comment, which a security policy has no
// clause for; a declaration with no predicate; and an expression that is not
// a call of a two-part function.
func (a Attributes) Policy() (objectidentity.ID, mssqlschema.DesiredSecurityPolicy, error) {
	ref, err := a.ref()
	if err != nil {
		return objectidentity.ID{}, mssqlschema.DesiredSecurityPolicy{}, err
	}
	if err := a.refuseUnheld(); err != nil {
		return objectidentity.ID{}, mssqlschema.DesiredSecurityPolicy{}, err
	}
	operation, err := a.operation()
	if err != nil {
		return objectidentity.ID{}, mssqlschema.DesiredSecurityPolicy{}, err
	}
	table := mssqlschema.ObjectName{Schema: a.TableSchema, Name: a.Table}
	if table.Schema == "" {
		table.Schema = DefaultSchema
	}
	policy := mssqlschema.DesiredSecurityPolicy{StructName: a.StructName}
	for _, clause := range []struct {
		text      string
		kind      mssqlschema.PredicateType
		operation mssqlschema.BlockOperation
	}{
		{text: a.Using, kind: mssqlschema.Filter},
		{text: a.WithCheck, kind: mssqlschema.Block, operation: operation},
	} {
		if strings.TrimSpace(clause.text) == "" {
			continue
		}
		function, arguments, err := mssqlschema.ParseInvocation(clause.text)
		if err != nil {
			return objectidentity.ID{}, mssqlschema.DesiredSecurityPolicy{}, fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err)
		}
		policy.Predicates = append(policy.Predicates, mssqlschema.Predicate{Type: clause.kind, Function: function,
			Arguments: arguments, Table: table, Operation: clause.operation})
	}
	if len(policy.Predicates) == 0 {
		return objectidentity.ID{}, mssqlschema.DesiredSecurityPolicy{}, fmt.Errorf(
			"%w: SQL Server security policy %s declares neither a USING nor a WITH CHECK predicate",
			ptaherr.ErrInvalidAttributeValue, a.Name)
	}
	if err := mssqlschema.ValidateDesiredSecurityPolicy(&policy); err != nil {
		return objectidentity.ID{}, mssqlschema.DesiredSecurityPolicy{}, fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err)
	}
	return ref, policy, nil
}

// ref is the policy's identity: the schema its name is qualified with, or its
// table's.
func (a Attributes) ref() (objectidentity.ID, error) {
	name, ok := tableref.Parse(a.Name)
	if !ok {
		return objectidentity.ID{}, fmt.Errorf("%w: %q is not a security policy name", ptaherr.ErrInvalidAttributeValue, a.Name)
	}
	schema := name.Schema
	if !name.Qualified {
		schema = a.TableSchema
	}
	if schema == "" {
		schema = DefaultSchema
	}
	ref := mssqlschema.SecurityPolicyRef(schema, name.Name)
	if err := mssqlschema.ValidateSecurityPolicyRef(ref); err != nil {
		return objectidentity.ID{}, fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err)
	}
	return ref, nil
}

// refuseUnheld refuses what a security policy has no clause for.
func (a Attributes) refuseUnheld() error {
	var unheld string
	switch {
	case strings.TrimSpace(a.To) != "":
		unheld = "TO " + a.To + ": SQL Server has no role list on a predicate, and scopes it inside the predicate function"
	case a.Restrictive:
		unheld = "AS RESTRICTIVE: a SQL Server security policy has no composition"
	case a.Comment != "":
		unheld = "a comment: a SQL Server security policy has no comment"
	default:
		return nil
	}
	return fmt.Errorf("%w: SQL Server security policy %s declares %s", ptaherr.ErrInvalidAttributeValue, a.Name, unheld)
}

// operation is the block predicate's operation FOR names.
func (a Attributes) operation() (mssqlschema.BlockOperation, error) {
	command := strings.ToUpper(strings.TrimSpace(a.For))
	operations := map[string]mssqlschema.BlockOperation{
		"": "", "ALL": "", "INSERT": mssqlschema.AfterInsert, "UPDATE": mssqlschema.AfterUpdate, "DELETE": mssqlschema.BeforeDelete,
	}
	operation, known := operations[command]
	switch {
	case !known:
		return "", fmt.Errorf("%w: SQL Server security policy %s declares FOR %s, which a security policy has no form for; "+
			"a filter predicate applies to every read and a block predicate fires on INSERT, UPDATE or DELETE",
			ptaherr.ErrInvalidAttributeValue, a.Name, command)
	case operation != "" && strings.TrimSpace(a.WithCheck) == "":
		return "", fmt.Errorf("%w: SQL Server security policy %s declares FOR %s, which only a block predicate can carry, "+
			"and declares no WITH CHECK; a filter predicate has no per-operation form, so the policy would cover every operation",
			ptaherr.ErrInvalidAttributeValue, a.Name, command)
	}
	return operation, nil
}

// Collector gathers one document's security policies. Declarations that name
// one policy are its predicates on several tables, which is how SQL Server
// holds one policy binding many tables; two that bind one slot, the same type
// and operation on one table, are refused, naming both. Its zero value is
// ready to use.
type Collector struct {
	policies map[objectidentity.Key]*collected
	order    []objectidentity.Key
}

type collected struct {
	ref     objectidentity.ID
	policy  mssqlschema.DesiredSecurityPolicy
	origins []string
	targets []string
}

// Add collects one declaration. origin names it in a refusal, in the source's
// own terms; targets is its target scope, which the object keeps.
func (c *Collector) Add(origin string, attributes Attributes, targets []string) error {
	ref, policy, err := attributes.Policy()
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	if c.policies == nil {
		c.policies = make(map[objectidentity.Key]*collected)
	}
	entry, found := c.policies[ref.Key()]
	if !found {
		c.policies[ref.Key()] = &collected{ref: ref, policy: policy, origins: []string{origin}, targets: slices.Clone(targets)}
		c.order = append(c.order, ref.Key())
		return nil
	}
	merged := entry.policy.Copy()
	merged.Predicates = append(merged.Predicates, policy.Predicates...)
	if err := mssqlschema.ValidateDesiredSecurityPolicy(merged); err != nil {
		return fmt.Errorf("%w: %s and %s both declare a predicate of security policy %s.%s on table %s; keep one declaration: %w",
			ptaherr.ErrInvalidAttributeValue, strings.Join(entry.origins, ", "), origin, ref.Schema.Source, ref.Name.Source,
			policy.Predicates[0].Table, err)
	}
	entry.policy = *merged
	entry.origins = append(entry.origins, origin)
	entry.targets = slices.Compact(slices.Sorted(slices.Values(append(entry.targets, targets...))))
	return nil
}

// Objects returns the collected policies, each keeping the targets its
// declarations are scoped to.
func (c *Collector) Objects() (schemaext.Objects, error) {
	objects := make([]schemaext.Object, 0, len(c.order))
	for _, key := range c.order {
		entry := c.policies[key]
		object, err := mssqlschema.DesiredSecurityPolicyObject(entry.ref, entry.policy)
		if err != nil {
			return schemaext.Objects{}, fmt.Errorf("%s: %w", strings.Join(entry.origins, ", "), err)
		}
		object.Targets = entry.targets
		objects = append(objects, object)
	}
	return schemaext.NewObjects(objects...)
}

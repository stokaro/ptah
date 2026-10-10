package pgpolicysource

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/tableref"
)

// Attributes is a policy as a text-attribute source spells it: every part a
// string, where an empty one is left out.
type Attributes struct {
	// For is the command, in any letter case. Empty requests ALL.
	For string
	// To is the role list in [ParseRoleList]'s grammar. Empty requests PUBLIC.
	To string
	// Using and WithCheck are the clause expressions. Empty declares no clause.
	Using, WithCheck string
	// Restrictive selects AS RESTRICTIVE.
	Restrictive bool
	Comment     string
	// StructName is the Go struct the declaration belongs to, if any.
	StructName string
}

// Policy reads the attributes as one policy declaration. A command or role
// list that is not one is refused, and so is a clause the command does not
// take.
func (a Attributes) Policy() (pgpolicy.DesiredPolicy, error) {
	policy := pgpolicy.DesiredPolicy{Comment: a.Comment, StructName: a.StructName}
	if command := strings.TrimSpace(a.For); command != "" {
		policy.Command = pgpolicy.Command(strings.ToUpper(command))
	}
	if strings.TrimSpace(a.To) != "" {
		roles, err := ParseRoleList(a.To)
		if err != nil {
			return pgpolicy.DesiredPolicy{}, err
		}
		policy.Roles = roles
	}
	if a.Using != "" {
		policy.Using = new(a.Using)
	}
	if a.WithCheck != "" {
		policy.WithCheck = new(a.WithCheck)
	}
	if a.Restrictive {
		policy.Composition = pgpolicy.Restrictive
	}
	if err := pgpolicy.ValidateDesiredPolicy(&policy); err != nil {
		return pgpolicy.DesiredPolicy{}, err
	}
	return policy, nil
}

// Collector gathers one document's row-security declarations for the owner
// and refuses two that share an identity, naming both. Its zero value is
// ready to use.
//
// Identity follows PostgreSQL's identifier rules, under which a table the
// declaration leaves unqualified is in the default schema: `orders` and
// `public.orders` name one table, and one policy name on it is one policy
// (stokaro/ptah#2440). Keeping either declaration would apply whichever came
// last, so neither is kept.
type Collector struct {
	objects schemaext.Objects
	origins map[objectidentity.Key]string
}

// Ref is the identity of the policy name declared on schema.table. An empty
// schema is the default one.
func Ref(schema, table, name string) objectidentity.ID {
	return pgpolicy.PolicyRefWith(identifier.ForDialect(platform.Postgres), schema, table, name)
}

// TableRef is the identity of the table schema.table, the subject of its
// row-security switches. An empty schema is the default one.
func TableRef(schema, table string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres)).TablePartsVerbatim(schema, table)
}

// AddPolicy collects one policy. origin names the declaration in a refusal,
// in the source's own terms. targets is the declaration's target scope, which
// the object keeps.
func (c *Collector) AddPolicy(origin string, ref objectidentity.ID, policy pgpolicy.DesiredPolicy, targets []string) error {
	if err := c.claim(origin, ref, fmt.Sprintf("policy %q on table %s", ref.Name.Source, tableText(pgpolicy.Table(ref)))); err != nil {
		return err
	}
	object, err := pgpolicy.DesiredPolicyObject(ref, policy)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	object.Targets = targets
	objects, err := c.objects.With(object)
	if err != nil {
		return fmt.Errorf("%s: %w", origin, err)
	}
	c.objects = objects
	return nil
}

// AddSwitches collects one table's switches as a facet of facets, the table's
// own. origin and targets have [Collector.AddPolicy]'s meaning.
func (c *Collector) AddSwitches(origin string, table objectidentity.ID, facets schemaext.Facets, state pgpolicy.DesiredTableState, targets []string) (schemaext.Facets, error) {
	if err := c.claim(origin, pgpolicy.TableStateSubject(table), "the row-level security switches of table "+tableText(table)); err != nil {
		return schemaext.Facets{}, err
	}
	if slices.Contains(facets.DeclaredKinds(), pgpolicy.TableStateKind) {
		return schemaext.Facets{}, fmt.Errorf("%w: %s: table %s declares its row-level security switches twice",
			ptaherr.ErrInvalidAttributeValue, origin, table)
	}
	if err := pgpolicy.ValidateDesiredTableState(&state); err != nil {
		return schemaext.Facets{}, fmt.Errorf("%s: %w", origin, err)
	}
	result, err := facets.With(&state)
	if err != nil {
		return schemaext.Facets{}, fmt.Errorf("%s: %w", origin, err)
	}
	if len(targets) == 0 {
		return result, nil
	}
	return result.WithTargetScope(pgpolicy.TableStateKind, targets...)
}

func (c *Collector) claim(origin string, ref objectidentity.ID, what string) error {
	if earlier, found := c.origins[ref.Key()]; found {
		return fmt.Errorf("%w: %s and %s both declare %s; keep one declaration",
			ptaherr.ErrInvalidAttributeValue, earlier, origin, what)
	}
	if c.origins == nil {
		c.origins = make(map[objectidentity.Key]string)
	}
	c.origins[ref.Key()] = origin
	return nil
}

// tableText names a table in a refusal as the declaration spelled it, with
// the default schema it was resolved in.
func tableText(table objectidentity.ID) string {
	return table.Schema.Source + "." + table.Name.Source
}

// Objects returns the collected policies.
func (c *Collector) Objects() schemaext.Objects { return c.objects }

// Coverage is what a desired document claims about row-level security once
// its declarations are collected: it describes every policy and every table's
// switches, except that a table with policies leaves its switches to the
// owner's default (see [pgpolicy.DefaultedSwitches]) where the document does
// not declare them. So a comparison drops a policy the document omits and turns
// row-level security off on a table it describes no policies for, and never
// disables a table whose policies it declares (stokaro/ptah#2048).
//
// The claim is made on the targets the table's policies are declared for: on
// every target when one of them is unscoped, and otherwise on the targets
// their scopes name. A switches facet scoped to fewer targets is absent on the
// rest, and the declared facet wins wherever it is present.
func Coverage(objects schemaext.Objects) (schemaext.Coverage, error) {
	all, err := objects.All()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	claims := make(map[objectidentity.Key]*schemaext.SubjectCoverage)
	var order []objectidentity.Key
	for _, object := range all {
		if object.Ref.Kind != objectidentity.Kind(pgpolicy.PolicyKind) {
			continue
		}
		table := pgpolicy.Table(object.Ref)
		claim, found := claims[table.Key()]
		if !found {
			claim = &schemaext.SubjectCoverage{Kind: pgpolicy.TableStateKind, Subject: table, Knowledge: pgpolicy.DefaultedSwitches(),
				Targets: slices.Clone(object.Targets)}
			claims[table.Key()] = claim
			order = append(order, table.Key())
			continue
		}
		if len(claim.Targets) == 0 || len(object.Targets) == 0 {
			claim.Targets = nil
			continue
		}
		claim.Targets = slices.Compact(slices.Sorted(slices.Values(append(claim.Targets, object.Targets...))))
	}
	defaulted := make([]schemaext.SubjectCoverage, 0, len(order))
	for _, key := range order {
		defaulted = append(defaulted, *claims[key])
	}
	complete := schemaext.Knowledge{State: schemaext.Complete}
	policies, err := pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, complete, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	switches, err := pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired, complete, defaulted)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return policies.Combine(switches)
}

// Claim adds [Coverage] for objects to coverage, a source's own claims once
// it has finished collecting its declarations. A source that already claims
// complete knowledge of both models, as one that enrolls them before reading
// any declaration does, keeps the claim and gains the defaulted switches.
func Claim(coverage schemaext.Coverage, objects schemaext.Objects) (schemaext.Coverage, error) {
	known, err := Coverage(objects)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	if coverage.Representation() == "" {
		return known, nil
	}
	var others []schemaext.Kind
	claimed := false
	for _, record := range coverage.KindRecords() {
		switch record.Model.Kind {
		case pgpolicy.PolicyKind, pgpolicy.TableStateKind:
			claimed = true
		default:
			others = append(others, record.Model.Kind)
		}
	}
	if claimed {
		// Merge intersects two sources' knowledge of every kind either names,
		// so it runs over this owner's kinds alone.
		owned, err := coverage.SelectKinds([]schemaext.Kind{pgpolicy.PolicyKind, pgpolicy.TableStateKind}).Merge(known)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		return coverage.SelectKinds(others).Combine(owned)
	}
	return coverage.Combine(known)
}

// DeclaredTable finds the table a row-security declaration names among a
// document's tables: the one structName maps to when table is empty, and
// otherwise the one of that name, in the schema a qualified reference names.
// It returns -1 when no table matches, and refuses a reference that more than
// one table matches, naming their schemas.
func DeclaredTable(tables []schemamodel.Table, structName, table string) (int, error) {
	if table == "" {
		return slices.IndexFunc(tables, func(declared schemamodel.Table) bool {
			return structName != "" && declared.StructName == structName
		}), nil
	}
	ref, ok := tableref.Parse(table)
	if !ok {
		return -1, fmt.Errorf("%w: %q is not a table reference", ptaherr.ErrInvalidAttributeValue, table)
	}
	var matches []int
	for i, declared := range tables {
		if declared.Name == ref.Name && (!ref.Qualified || declared.Schema == ref.Schema) {
			matches = append(matches, i)
		}
	}
	switch len(matches) {
	case 0:
		return -1, nil
	case 1:
		return matches[0], nil
	}
	schemas := make([]string, 0, len(matches))
	for _, i := range matches {
		schemas = append(schemas, strconv.Quote(cmp.Or(tables[i].Schema, "(default)")))
	}
	return -1, fmt.Errorf("%w: table %q is declared in schemas %s; name the schema",
		ptaherr.ErrInvalidAttributeValue, table, strings.Join(schemas, " and "))
}

// TableParts returns the schema and name a written table reference names.
func TableParts(table string) (schema, name string, err error) {
	ref, ok := tableref.Parse(table)
	if !ok {
		return "", "", fmt.Errorf("%w: %q is not a table reference", ptaherr.ErrInvalidAttributeValue, table)
	}
	return ref.Schema, ref.Name, nil
}
